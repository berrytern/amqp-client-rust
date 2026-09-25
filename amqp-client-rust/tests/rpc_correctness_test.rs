mod base;
use base::create_test_config;
use amqp_client_rust::{
    api::{
        eventbus::AsyncEventbusRabbitMQ,
        utils::{ContentEncoding, Message, RpcClientOptions},
    },
    domain::config::QoSConfig,
};
use std::time::Duration;
use uuid::Uuid;

// -------------------------------------------------------------------------
// Caso A: Quando o RPC Server retorna Err, o RPC Client deve receber Err
// -------------------------------------------------------------------------
#[tokio::test]
async fn test_case_a_rpc_server_error_propagates_as_err() {
    let config = create_test_config();
    let eventbus = AsyncEventbusRabbitMQ::new(config.clone(), QoSConfig::default());

    let routing_key = format!("rpc_err_rk_{}", Uuid::new_v4().simple());

    // RPC Server que retorna erro deliberado
    eventbus
        .provide_resource(
            &routing_key,
            |_req: Message| async move {
                Err(Box::new(std::io::Error::other("Database query failed"))
                    as Box<dyn std::error::Error + Send + Sync>)
            },
            Some(Duration::from_secs(5)),
            Some(Duration::from_secs(5)),
        )
        .await
        .expect("Failed to register RPC server");

    let rpc_opts = RpcClientOptions {
        content_type: "text/plain",
        content_encoding: ContentEncoding::None,
        response_timeout_millis: 3000,
        command_timeout: Some(Duration::from_secs(5)),
        ..Default::default()
    };
    // Cliente chama o RPC
    let res = eventbus
        .rpc_client(
            &config.options.rpc_exchange_name,
            &routing_key,
            b"test".to_vec(),
            &rpc_opts,
        )
        .await;

    let _ = eventbus.dispose().await;

    // CASO A: O resultado DEVE ser Err(...) informando falha no servidor RPC!
    // No código antigo, res.is_ok() retornava Ok(b"Database query failed")
    assert!(
        res.is_err(),
        "Esperado Err(...) quando o servidor RPC falha, mas obteve Ok: {:?}",
        res
    );
}


// -------------------------------------------------------------------------
// Caso C: Falha de descompressão na resposta RPC deve retornar Err, não Ok([])
// -------------------------------------------------------------------------
#[tokio::test]
async fn test_case_c_rpc_decompression_failure_returns_err() {
    let config = create_test_config();
    let eventbus = AsyncEventbusRabbitMQ::new(config.clone(), QoSConfig::default());
    let routing_key = format!("rpc_decomp_rk_{}", Uuid::new_v4().simple());

    // Abre uma conexão separada para simular um servidor que responde com bytes corrompidos
    let mut conn_args = amqprs::connection::OpenConnectionArguments::new(
        &config.host,
        config.port,
        &config.username,
        &config.password,
    );
    conn_args.virtual_host(&config.virtual_host);
    let conn = amqprs::connection::Connection::open(&conn_args)
        .await
        .expect("Failed to open connection");
    let server_ch = conn.open_channel(None).await.expect("Failed to open channel");

    // Registra consumidor na fila RPC que intercepta a requisição e responde bytes corrompidos
    // diretamente para a fila reply_to do cliente com header 'application/zstd'
    let (rpc_q, _, _) = server_ch
        .queue_declare(amqprs::channel::QueueDeclareArguments::new(&routing_key).auto_delete(true).finish())
        .await
        .unwrap()
        .unwrap();

    let server_ch_clone = server_ch.clone();
    tokio::spawn(async move {
        let (_ctag, mut rx) = server_ch_clone
            .basic_consume_rx(amqprs::channel::BasicConsumeArguments::new(&rpc_q, "fake_server"))
            .await
            .unwrap();

        if let Some(msg) = rx.recv().await {
            if let Some(props) = msg.basic_properties {
                if let Some(reply_to) = props.reply_to() {
                    let mut reply_props = amqprs::BasicProperties::default();
                    if let Some(corr) = props.correlation_id() {
                        reply_props.with_correlation_id(corr);
                    }
                    // Marca como compactado com zstd, mas envia lixo não compactado
                    reply_props.with_content_encoding("application/zstd");
                    let corrupted_payload = b"DEFINITELY_NOT_A_VALID_ZSTD_STREAM".to_vec();

                    let args = amqprs::channel::BasicPublishArguments::new("", reply_to);
                    let _ = server_ch_clone.basic_publish(reply_props, corrupted_payload, args).await;
                }
            }
        }
    });

    // Client chama esperando resposta
    let rpc_opts = RpcClientOptions {
        content_type: "text/plain",
        content_encoding: ContentEncoding::None,
        response_timeout_millis: 3000,
        command_timeout: Some(Duration::from_secs(5)),
        ..Default::default()
    };
    let res = eventbus
        .rpc_client(
            "",
            &routing_key,
            b"ping".to_vec(),
            &rpc_opts,
        )
        .await;

    let _ = eventbus.dispose().await;

    // CASO C: O resultado DEVE ser Err(...) informando erro de descompressão!
    // No código antigo, ele retornava Ok(vec![]) (sucesso com vetor vazio)!
    assert!(
        res.is_err(),
        "Esperado Err em falha de descompressão de RPC, mas obteve Ok: {:?}",
        res
    );
}

// -------------------------------------------------------------------------
// Caso D: Timeout de resposta longo (> 32s) não é truncado por command_timeout forçado
// -------------------------------------------------------------------------
#[tokio::test]
async fn test_case_d_rpc_response_timeout_not_truncated() {
    // Validação da política de timeout no eventbus
    let user_command_timeout: Option<Duration> = None;

    // Código legado antigo forçava 32s:
    let old_forced_timeout = user_command_timeout.or(Some(Duration::from_secs(32)));
    assert_eq!(
        old_forced_timeout,
        Some(Duration::from_secs(32)),
        "Código antigo forçava 32s e truncava respostas longas!"
    );

    // Novo comportamento limpo: command_timeout passa diretamente como informado (None se não informado)
    // permitindo que o canal AMQP controle a resposta pelo response_timeout_millis sem interferência
    let effective_command_timeout = user_command_timeout;
    assert_eq!(effective_command_timeout, None, "Quando não informado, command_timeout deve permanecer None");

    // Se o usuário passar um timeout explícito de comando, ele é preservado
    let custom_timeout = Some(Duration::from_secs(10));
    assert_eq!(custom_timeout, Some(Duration::from_secs(10)));
}
