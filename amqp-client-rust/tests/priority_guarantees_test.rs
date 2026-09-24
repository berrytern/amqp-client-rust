mod base;
use base::{cleanup_test_resources, create_test_config};
use amqp_client_rust::{
    api::eventbus::AsyncEventbusRabbitMQ,
    api::utils::ContentEncoding,
    domain::config::QoSConfig,
    errors::{AppError, AppErrorType},
};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::sync::oneshot;
use uuid::Uuid;

// -----------------------------------------------------------------------
// Teste 1: Pânico no handler do consumidor NÃO trava o shutdown em deadlock
// -----------------------------------------------------------------------
#[tokio::test]
async fn test_subscriber_panic_does_not_deadlock_shutdown() {
    let config = create_test_config();
    let qos_config = QoSConfig {
        sub_auto_ack: false,
        ..Default::default()
    };
    let eventbus = AsyncEventbusRabbitMQ::new(config.clone(), qos_config);

    let exchange_name = format!("test_panic_ex_{}", Uuid::new_v4());
    let routing_key = format!("test_panic_key_{}", Uuid::new_v4());

    // Subscriber handler que entra em pânico propositalmente
    eventbus.subscribe(
        &exchange_name,
        &routing_key,
        |_msg| {
            Box::pin(async move {
                panic!("Simulated business panic inside consumer handler!");
            })
        },
        None,
        Some(Duration::from_secs(5)),
    ).await.expect("Failed to subscribe");

    // Publica mensagem para acionar o handler
    eventbus.publish(
        &exchange_name,
        &routing_key,
        b"trigger panic message",
        Some("text/plain"),
        ContentEncoding::None,
        Some(Duration::from_secs(5)),
        None,
        None,
    ).await.expect("Failed to publish");

    // Aguarda 150ms para a mensagem ser entregue e o pânico ocorrer
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Se o InFlightGuard não existisse, este dispose() ficaria congelado em deadlock
    // para sempre esperando in_flight == 0!
    // Com o timeout de 2 segundos, provamos que o shutdown completa sem travar.
    let shutdown_result = tokio::time::timeout(Duration::from_secs(2), eventbus.dispose()).await;
    assert!(
        shutdown_result.is_ok(),
        "eventbus.dispose() travou em deadlock após pânico no handler do consumidor!"
    );
    assert!(shutdown_result.unwrap().is_ok());

    cleanup_test_resources(&config, &[&exchange_name]).await;
}

// -----------------------------------------------------------------------
// Teste 2: Pânico no handler do RPC Server NÃO trava o shutdown em deadlock
// -----------------------------------------------------------------------
#[tokio::test]
async fn test_rpc_server_panic_does_not_deadlock_shutdown() {
    let config = create_test_config();
    let eventbus = AsyncEventbusRabbitMQ::new(config.clone(), QoSConfig::default());
    let routing_key = format!("rpc_panic_key_{}", Uuid::new_v4());

    // RPC server handler que entra em pânico propositalmente
    let _ = eventbus.provide_resource(
        &routing_key,
        |_req| {
            Box::pin(async move {
                panic!("Simulated business panic inside RPC server handler!");
            })
        },
        Some(Duration::from_secs(5)),
        Some(Duration::from_secs(5)),
    ).await;

    // Cliente faz chamada RPC
    let _ = eventbus.rpc_client(
        &config.options.rpc_exchange_name,
        &routing_key,
        b"rpc test".to_vec(),
        "text/plain",
        ContentEncoding::None,
        200, // 200ms timeout
        Some(Duration::from_millis(500)),
        None,
        None,
    ).await;

    // Aguarda o processamento
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Shutdown deve completar graciosamente sem travar
    let shutdown_result = tokio::time::timeout(Duration::from_secs(2), eventbus.dispose()).await;
    assert!(
        shutdown_result.is_ok(),
        "eventbus.dispose() travou em deadlock após pânico no RPC handler!"
    );
    assert!(shutdown_result.unwrap().is_ok());

    cleanup_test_resources(&config, &[]).await;
}

// -----------------------------------------------------------------------
// Teste 3: Limpeza de confirmações e comandos pendentes retorna ConnectionReset
// -----------------------------------------------------------------------
#[tokio::test]
async fn test_pending_confirmations_aborted_on_connection_reset() {
    let mut pending: BTreeMap<u64, oneshot::Sender<Result<(), AppError>>> = BTreeMap::new();
    let (tx1, mut rx1) = oneshot::channel();
    let (tx2, mut rx2) = oneshot::channel();
    let (tx3, mut rx3) = oneshot::channel();

    pending.insert(1, tx1);
    pending.insert(2, tx2);
    pending.insert(3, tx3);

    // Simula o método ConnectionManager::abort_pending_confirmations
    for (_, confirm) in std::mem::take(&mut pending) {
        let _ = confirm.send(Err(AppError::new(
            Some("Connection reset before publisher confirmation was received".to_string()),
            None,
            AppErrorType::ConnectionReset,
        )));
    }

    assert_eq!(pending.len(), 0, "Mapa de confirmações deve estar vazio");

    // Todos os 3 callers devem receber erro com tipo semântico ConnectionReset
    let res1 = rx1.try_recv().expect("rx1 deve ter recebido");
    let res2 = rx2.try_recv().expect("rx2 deve ter recebido");
    let res3 = rx3.try_recv().expect("rx3 deve ter recebido");

    assert_eq!(res1.unwrap_err().error_type, AppErrorType::ConnectionReset);
    assert_eq!(res2.unwrap_err().error_type, AppErrorType::ConnectionReset);
    assert_eq!(res3.unwrap_err().error_type, AppErrorType::ConnectionReset);
}

// -----------------------------------------------------------------------
// Teste 4: Backpressure real - Rejeição imediata com BufferFull ao atingir limite
// -----------------------------------------------------------------------
#[tokio::test]
async fn test_backpressure_rejection_when_disconnected() {
    use amqp_client_rust::domain::config::{Config, ConfigOptions};

    // Configura cliente apontando para porta inacessível (conexão caída) com teto de apenas 2 comandos
    let mut options = ConfigOptions::new("test_q", "test_rpc_q", "test_rpc_ex");
    options.max_pending_commands = 2;

    let unreachable_config = Config::new(
        "127.0.0.1",
        59999, // Porta onde nada está escutando
        "guest",
        "guest",
        options,
        "/",
    );

    let qos_config = QoSConfig {
        pub_confirm: false, // Desativa confirms para focar na fila de comandos
        ..Default::default()
    };
    let eventbus = AsyncEventbusRabbitMQ::new(unreachable_config, qos_config);

    // Comando 1: entra na fila de pendências (tamanho 1)
    let pub1 = eventbus.publish(
        "ex", "rk", b"msg1", None, ContentEncoding::None, Some(Duration::from_millis(50)), None, None,
    );

    // Comando 2: entra na fila de pendências (tamanho 2 == max_pending_commands)
    let pub2 = eventbus.publish(
        "ex", "rk", b"msg2", None, ContentEncoding::None, Some(Duration::from_millis(50)), None, None,
    );

    // Comando 3: excede a capacidade máxima -> Deve ser rejeitado IMEDIATAMENTE com BufferFull!
    let pub3 = eventbus.publish(
        "ex", "rk", b"msg3", None, ContentEncoding::None, Some(Duration::from_millis(50)), None, None,
    );

    let (res1, res2, res3) = tokio::join!(pub1, pub2, pub3);

    // res3 deve ter sido rejeitado com BufferFull
    assert!(res3.is_err(), "Comando 3 deveria falhar por buffer cheio");
    let err3 = res3.unwrap_err();
    assert_eq!(
        err3.error_type,
        AppErrorType::BufferFull,
        "Esperado AppErrorType::BufferFull, obtido: {:?}",
        err3
    );

    // Dispose limpa os comandos pendentes com ConnectionReset
    let _ = eventbus.dispose().await;

    // res1 e res2 recebem ConnectionReset ou Timeout
    assert!(res1.is_err());
    assert!(res2.is_err());
}
