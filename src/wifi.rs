use crate::CONFIG;
use crate::EVENTS;
use crate::Event;
use crate::HOSTNAME;
use crate::STACK_NUM_SOCKETS;
use crate::STATE;
use crate::WifiStatus;
use crate::mk_static;
use embassy_executor::Spawner;
use embassy_net::DhcpConfig;
use embassy_net::{Runner, Stack, StackResources};
use embassy_time::WithTimeout;
use embassy_time::{Duration, Timer};
use esp_hal::rng::Rng;
use esp_radio::wifi::AuthMethod;
use esp_radio::wifi::ClientConfig;
use esp_radio::wifi::ModeConfig;
use esp_radio::wifi::PowerSaveMode;
use esp_radio::wifi::WifiStaState;
use esp_radio::wifi::{Interfaces, WifiController, WifiDevice, WifiEvent};
use tracing::info;

pub fn init_wifi_stack(
    spawner: Spawner,
    rng: Rng,
    mut controller: WifiController<'static>,
    interfaces: Interfaces<'static>,
) -> Stack<'static> {
    controller.set_power_saving(PowerSaveMode::Minimum).unwrap();

    let wifi_interface = interfaces.sta;
    let mut dhcp_config: DhcpConfig = Default::default();
    dhcp_config.hostname = Some(HOSTNAME.try_into().unwrap());
    let config = embassy_net::Config::dhcpv4(dhcp_config);
    let seed = (rng.random() as u64) << 32 | rng.random() as u64;

    let (stack, runner) = embassy_net::new(
        wifi_interface,
        config,
        mk_static!(
            StackResources<STACK_NUM_SOCKETS>,
            StackResources::<_>::new()
        ),
        seed,
    );

    spawner.spawn(connection(controller)).ok();
    spawner.spawn(net_task(runner)).ok();
    stack
}

#[embassy_executor::task]
async fn connection(mut controller: WifiController<'static>) {
    let sender = EVENTS.publisher().unwrap();
    info!("start connection task");
    info!("Device capabilities: {:?}", controller.capabilities());
    loop {
        if esp_radio::wifi::sta_state() == WifiStaState::Connected {
            // wait until we're no longer connected
            match controller
                .wait_for_event(WifiEvent::StaDisconnected)
                .with_timeout(Duration::from_secs(10))
                .await
            {
                Ok(_) => {}
                Err(_) => {
                    // still connected
                    // check if we should terminate
                    if let Some(state) = STATE.try_get()
                        && state.terminated
                    {
                        controller
                            .disconnect_async()
                            .await
                            .expect("Failed to disconnect wifi");
                        info!("Wifi disconnected.");
                        controller
                            .stop_async()
                            .await
                            .expect("Failed to stop wifi controller");
                        info!("Wifi controller stopped.");
                        break;
                    }
                    continue;
                }
            };
            sender.publish(Event::Wifi(WifiStatus::Disconnected)).await;
            info!("Wifi disconnected!");
            Timer::after(Duration::from_millis(5000)).await
        }

        let (ssid, password) = { (CONFIG.wifi.ssid, CONFIG.wifi.password) };

        let auth_method = if password.is_empty() {
            AuthMethod::None
        } else {
            AuthMethod::default()
        };

        if !matches!(controller.is_started(), Ok(true)) {
            let client_config = ModeConfig::Client(
                ClientConfig::default()
                    .with_ssid(ssid.into())
                    .with_password(password.into())
                    .with_auth_method(auth_method),
            );
            controller.set_config(&client_config).unwrap();
            controller.start_async().await.unwrap();
            sender.publish(Event::Wifi(WifiStatus::Connecting)).await;
            info!("Connecting to {ssid} ...");
        }

        match controller.connect_async().await {
            Ok(_) => {
                sender.publish(Event::Wifi(WifiStatus::Connected)).await;
                info!("Wifi connected!")
            }
            Err(e) => {
                info!("Failed to connect: {e:?}");
                Timer::after(Duration::from_millis(5000)).await
            }
        }
    }

    // if we exit the wifi controller get's dropped, which we want to avoid for now
    loop {
        Timer::after(Duration::from_secs(10)).await;
    }
}

#[embassy_executor::task]
async fn net_task(mut runner: Runner<'static, WifiDevice<'static>>) {
    runner.run().await
}
