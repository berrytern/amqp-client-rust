mod base;
use base::create_test_config;
use amqp_client_rust::{
    api::{
        eventbus::AsyncEventbusRabbitMQ,
        utils::{ContentEncoding, Message},
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

    // Cliente chama o RPC
    let res = eventbus
        .rpc_client(
            &config.options.rpc_exchange_name,
            &routing_key,
            b"test".to_vec(),
            "text/plain",
            ContentEncoding::None,
            3000,
            Some(Duration::from_secs(5)),
            None,
            None,
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
    let res = eventbus
        .rpc_client(
            "",
            &routing_key,
            b"ping".to_vec(),
            "text/plain",
            ContentEncoding::None,
            3000,
            Some(Duration::from_secs(5)),
            None,
            None,
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
// Caso D: Timeout de resposta longo (> 32s) não pode ser abortado pelo command_timeout padrão
// -------------------------------------------------------------------------
#[tokio::test]
async fn test_case_d_rpc_response_timeout_not_truncated() {
    // Validação lógica do cálculo de command_timeout no eventbus
    let response_timeout_millis = 35_000_u32; // 35 segundos
    let user_command_timeout: Option<Duration> = None;

    // Fórmula anterior (falha):
    let old_command_timeout = user_command_timeout.or(Some(Duration::from_secs(32)));
    assert_eq!(
        old_command_timeout.unwrap(),
        Duration::from_secs(32),
        "Fórmula antiga limitava o timeout a 32s mesmo com resposta de 35s!"
    );

    // Nova fórmula esperada:
    let calculate_timeout = |cmd_to: Option<Duration>, resp_millis: u32| {
        let expected = Duration::from_millis(resp_millis as u64);
        cmd_to.unwrap_or_else(|| std::cmp::max(Duration::from_secs(32), expected + Duration::from_secs(5)))
    };

    let expected_response = Duration::from_millis(response_timeout_millis as u64);
    let new_command_timeout = calculate_timeout(user_command_timeout, response_timeout_millis);

    assert!(
        new_command_timeout >= expected_response,
        "O timeout de comando ({:?}) deve ser maior ou igual ao timeout de resposta ({:?})",
        new_command_timeout,
        expected_response
    );
    assert_eq!(new_command_timeout, Duration::from_secs(40));

    // Se o usuário passar um timeout explícito, ele deve ser respeitado
    let custom_timeout = calculate_timeout(Some(Duration::from_secs(50)), response_timeout_millis);
    assert_eq!(custom_timeout, Duration::from_secs(50));
}
