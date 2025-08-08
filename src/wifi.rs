#[cfg(not(feature = "wokwi"))]
use crate::CONFIG;
use crate::EVENTS;
use crate::Event;
use crate::mk_static;
use embassy_executor::Spawner;
use embassy_net::{Runner, Stack, StackResources};
use embassy_time::{Duration, Timer};
use esp_hal::rng::Rng;
use esp_wifi::wifi::AuthMethod;
use esp_wifi::{
    config::PowerSaveMode,
    wifi::{
        ClientConfiguration, Configuration, Interfaces, WifiController, WifiDevice, WifiEvent,
        WifiState,
    },
};
use log::info;

pub fn init_wifi_stack(
    spawner: Spawner,
    mut rng: Rng,
    mut controller: WifiController<'static>,
    interfaces: Interfaces<'static>,
) -> Stack<'static> {
    controller.set_power_saving(PowerSaveMode::Minimum).unwrap();

    let wifi_interface = interfaces.sta;
    let config = embassy_net::Config::dhcpv4(Default::default());
    let seed = (rng.random() as u64) << 32 | rng.random() as u64;

    let (stack, runner) = embassy_net::new(
        wifi_interface,
        config,
        mk_static!(StackResources<3>, StackResources::<3>::new()),
        seed,
    );

    spawner.spawn(connection(controller)).ok();
    spawner.spawn(net_task(runner)).ok();
    stack
}

#[embassy_executor::task]
async fn connection(mut controller: WifiController<'static>) {
    let sender = EVENTS.sender();
    info!("start connection task");
    info!("Device capabilities: {:?}", controller.capabilities());
    loop {
        if esp_wifi::wifi::wifi_state() == WifiState::StaConnected {
            // wait until we're no longer connected
            controller.wait_for_event(WifiEvent::StaDisconnected).await;
            sender.send(Event::Connection(false)).await;
            info!("Wifi disconnected!");
            Timer::after(Duration::from_millis(5000)).await
        }

        #[cfg(not(feature = "wokwi"))]
        let (ssid, password) = { (CONFIG.wifi.ssid, CONFIG.wifi.password) };
        #[cfg(feature = "wokwi")]
        let (ssid, password) = { ("Wokwi-GUEST", "") };

        let auth_method = if password.is_empty() {
            AuthMethod::None
        } else {
            AuthMethod::default()
        };

        if !matches!(controller.is_started(), Ok(true)) {
            let client_config = Configuration::Client(ClientConfiguration {
                ssid: ssid.into(),
                password: password.into(),
                auth_method,
                ..Default::default()
            });
            controller.set_configuration(&client_config).unwrap();
            controller.start_async().await.unwrap();
            info!("Wifi started!");
        }
        info!("Connecting to {ssid} ...");

        match controller.connect_async().await {
            Ok(_) => {
                sender.send(Event::Connection(true)).await;
                info!("Wifi connected!")
            }
            Err(e) => {
                info!("Failed to connect: {e:?}");
                Timer::after(Duration::from_millis(5000)).await
            }
        }
    }
}

#[embassy_executor::task]
async fn net_task(mut runner: Runner<'static, WifiDevice<'static>>) {
    runner.run().await
}
