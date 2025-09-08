pub mod dummy_agent;

use std::{cell::{RefCell}, collections::HashMap, io::Write, sync::Arc};

use agent_client_protocol::{self as acp, ContentBlock, SessionNotification, TextContent};
use crossterm::{
    execute, queue, style::{self, Color}
};
use tokio::sync::{mpsc, oneshot};

use crate::{
    cli::{
        agent::Agents,
        chat::{
            cli::model::{find_model, get_available_models}, input_source::InputSource, tool_manager::{PromptQuery, PromptQueryResult, ToolManagerBuilder}, tools::NATIVE_TOOLS, ChatArgs, ChatSession, ChatState
        },
    },
    database::settings::Setting,
    os::Os,
};
use tracing::error;

pub struct QAgent {
    session_update_tx: mpsc::UnboundedSender<(acp::SessionNotification, oneshot::Sender<()>)>,
    sessions: RefCell<HashMap<String, ChatSession>>,
    chat_args: ChatArgs,
    os: RefCell<Os>,
    
    agents: Agents,
    mcp_enabled: bool,
}

impl QAgent {
    pub async fn new(session_update_tx: mpsc::UnboundedSender<(acp::SessionNotification, oneshot::Sender<()>)>, chat_args: ChatArgs, mut os: Os) -> Self {
        let stdout = std::io::stdout();
        let mut stderr = std::io::stderr();

        let mcp_enabled = os.client.is_mcp_enabled().await.unwrap();

        let agents = {
            let skip_migration = false;
            let (mut agents, md) = Agents::load(&mut os, None, skip_migration, &mut stderr, mcp_enabled).await;
            agents.trust_all_tools = false;

            // // this needs session id so need to put in new session
            // self.os.telemetry
            //     .send_agent_config_init(&self.os.database, session_id.clone(), AgentConfigInitArgs {
            //         agents_loaded_count: md.load_count as i64,
            //         agents_loaded_failed_count: md.load_failed_count as i64,
            //         legacy_profile_migration_executed: md.migration_performed,
            //         legacy_profile_migrated_count: md.migrated_count as i64,
            //         launched_agent: md.launched_agent,
            //     })
            //     .await
            //     .map_err(|err| error!(?err, "failed to send agent config init telemetry"))
            //     .ok();

            // Only show MCP safety message if MCP is enabled and has servers
            if mcp_enabled
                && agents
                    .get_active()
                    .is_some_and(|a| !a.mcp_servers.mcp_servers.is_empty())
            {
                if !os.database.settings.get_bool(Setting::McpLoadedBefore).unwrap_or(false) {
                    execute!(
                        stderr,
                        style::Print(
                            "To learn more about MCP safety, see https://docs.aws.amazon.com/amazonq/latest/qdeveloper-ug/command-line-mcp-security.html\n\n"
                        )
                    ).unwrap();
                }
                os.database.settings.set(Setting::McpLoadedBefore, true).await.unwrap();
            }

            match chat_args.trust_tools {
                Some(ref trust_tools) => {
                    for tool in trust_tools {
                        if !tool.starts_with("@") && !NATIVE_TOOLS.contains(&tool.as_str()) {
                            let _ = queue!(
                                stderr,
                                style::SetForegroundColor(Color::Yellow),
                                style::Print("WARNING: "),
                                style::SetForegroundColor(Color::Reset),
                                style::Print("--trust-tools arg for custom tool "),
                                style::SetForegroundColor(Color::Cyan),
                                style::Print(tool),
                                style::SetForegroundColor(Color::Reset),
                                style::Print(" needs to be prepended with "),
                                style::SetForegroundColor(Color::Green),
                                style::Print("@{MCPSERVERNAME}/"),
                                style::SetForegroundColor(Color::Reset),
                                style::Print("\n"),
                            );
                        }
                    }

                    let _ = stderr.flush();

                    if let Some(a) = agents.get_active_mut() {
                        a.allowed_tools.extend(trust_tools.clone());
                    }
                },
                _ => {},
            }

            agents
        };

        Self {
            session_update_tx,
            sessions: RefCell::new(HashMap::new()),
            chat_args,
            os: RefCell::new(os),
            agents,
            mcp_enabled,
        }
    }
}

impl acp::Agent for QAgent {
    async fn initialize(
        &self,
        args: agent_client_protocol::InitializeRequest,
    ) -> anyhow::Result<acp::InitializeResponse, acp::Error> {
        Ok(acp::InitializeResponse {
            protocol_version: args.protocol_version,
            auth_methods: [
                // TODO
                acp::AuthMethod {
                    id: acp::AuthMethodId(Arc::from("TODO")),
                    name: "TODO".to_string(),
                    description: Some("TODO".to_string()),
                },
            ]
            .to_vec(),
            agent_capabilities: agent_client_protocol::AgentCapabilities {
                load_session: false,
                prompt_capabilities: agent_client_protocol::PromptCapabilities {
                    image: true,
                    audio: true,
                    embedded_context: true,
                },
            },
        })
    }

    async fn authenticate(&self, _args: agent_client_protocol::AuthenticateRequest) -> anyhow::Result<(), acp::Error> {
        Err(acp::Error::method_not_found())
    }

    async fn new_session(
        &self,
        args: agent_client_protocol::NewSessionRequest,
    ) -> anyhow::Result<acp::NewSessionResponse, acp::Error> {
        let session_id = uuid::Uuid::new_v4().to_string();

        let mut stderr = std::io::stderr();

        let (prompt_request_sender, prompt_request_receiver) = tokio::sync::broadcast::channel::<PromptQuery>(5);
        let (prompt_response_sender, prompt_response_receiver) =
            tokio::sync::broadcast::channel::<PromptQueryResult>(5);

        let mut tool_manager = ToolManagerBuilder::default()
            .prompt_query_result_sender(prompt_response_sender)
            .prompt_query_receiver(prompt_request_receiver)
            .prompt_query_sender(prompt_request_sender.clone())
            .prompt_query_result_receiver(prompt_response_receiver.resubscribe())
            .conversation_id(&session_id)
            .agent(self.agents.get_active().cloned().unwrap_or_default())
            .build(&self.os.borrow(), Box::new(std::io::stderr()), true)
            .await
            .unwrap();

        let tool_config = tool_manager.load_tools(&self.os.borrow(), &mut stderr).await.unwrap();

        let input_source = InputSource::new(&self.os.borrow(), prompt_request_sender, prompt_response_receiver).unwrap();

        // If modelId is specified, verify it exists before starting the chat
        // Otherwise, CLI will use a default model when starting chat
        let (models, default_model_opt) = get_available_models(&self.os.borrow()).await.unwrap();
        let model_id: Option<String> = if let Some(requested) = self.chat_args.model.as_ref() {
            if let Some(m) = find_model(&models, requested) {
                Some(m.model_id.clone())
            } else {
                let available = models
                    .iter()
                    .map(|m| m.model_name.as_deref().unwrap_or(&m.model_id))
                    .collect::<Vec<_>>()
                    .join(", ");
                error!("Model '{}' does not exist. Available models: {}", requested, available);
                None
            }
        } else if let Some(saved) = self.os.borrow().database.settings.get_string(Setting::ChatDefaultModel) {
            find_model(&models, &saved)
                .map(|m| m.model_id.clone())
                .or(Some(default_model_opt.model_id.clone()))
        } else {
            Some(default_model_opt.model_id.clone())
        };

        // Need to create ChatSession in here
        let chat_session = ChatSession::new(
            &self.os.borrow(),
            std::io::stdout(),
            std::io::stderr(),
            &session_id,
            self.agents.clone(),
            self.chat_args.input.clone(),
            input_source,
            self.chat_args.resume,
            || None,
            tool_manager,
            model_id,
            tool_config,
            !self.chat_args.no_interactive,
            self.mcp_enabled,
        )
        .await
        .unwrap();

        self.sessions.borrow_mut().insert(session_id.clone(), chat_session);

        Ok(acp::NewSessionResponse {
            session_id: acp::SessionId(Arc::from(session_id.clone())),
        })
    }

    async fn load_session(&self, _args: agent_client_protocol::LoadSessionRequest) -> anyhow::Result<(), acp::Error> {
        Err(acp::Error::method_not_found())
    }

    async fn prompt(
        &self,
        args: agent_client_protocol::PromptRequest,
    ) -> anyhow::Result<acp::PromptResponse, acp::Error> {
        let mut sessions = self.sessions.borrow_mut();
        let chat_session = sessions.get_mut(&args.session_id.to_string()).unwrap();

        let prompt_inputs: Vec<String> = args.prompt.iter().filter_map(
            |block| match block {
                agent_client_protocol::ContentBlock::Text(block) => Some(block.text.clone()),
                agent_client_protocol::ContentBlock::ResourceLink(block) => Some(block.uri.clone()),
                _ => None,
            }, 
        ).collect();

        for input in prompt_inputs {
            chat_session.inner = Some(ChatState::HandleInput { input });

            while !matches!(chat_session.inner, Some(ChatState::PromptUser { skip_printing_tools: false })) {
                chat_session.next(&mut *self.os.borrow_mut()).await;
            }

            //TODO: handle tool use approval

            let content = ContentBlock::Text(TextContent {
                annotations: None,
                text: chat_session.conversation.transcript.back().unwrap().clone(),
            });

            let (tx, rx) = oneshot::channel();
            self.session_update_tx
                .send((
                    SessionNotification {
                        session_id: args.session_id.clone(),
                        update: acp::SessionUpdate::AgentMessageChunk { content },
                    },
                    tx,
                ))
                .map_err(|_| acp::Error::internal_error())?;
            rx.await.map_err(|_| acp::Error::internal_error())?;
        }

        Ok(acp::PromptResponse {
            stop_reason: acp::StopReason::EndTurn,
        })
    }

    async fn cancel(&self, _args: agent_client_protocol::CancelNotification) -> anyhow::Result<(), acp::Error> {
        Err(acp::Error::method_not_found())
    }
}
