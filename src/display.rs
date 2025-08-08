use airflow_common::datetime::TimeProvider;
use core::fmt::Write;
use display_interface::DisplayError;
use embedded_graphics::{
    image::Image,
    mono_font::{
        MonoTextStyle, MonoTextStyleBuilder,
        ascii::{FONT_5X7, FONT_9X18_BOLD},
    },
    pixelcolor::BinaryColor,
    prelude::*,
    text::{Alignment, Text},
};
use embedded_iconoir::icons::size16px::communication::Internet;
use embedded_iconoir::icons::size16px::connectivity::{PrivateWifi, WifiOff};
use embedded_iconoir::prelude::*;
use esp_hal::{DriverMode, i2c::master::I2c};
use ssd1306::{I2CDisplayInterface, Ssd1306, prelude::*};

use crate::{State, TIME_PROVIDER, WifiStatus};

static DELTA_Y: i32 = 9;
static V_SPACE: i32 = 1;
const DIM_X: u8 = 128;
const ICON_SIZE: u8 = 16;

type DisplayType<'a, I> = Ssd1306<
    I2CInterface<I2c<'a, I>>,
    DisplaySize128x64,
    ssd1306::mode::BufferedGraphicsMode<DisplaySize128x64>,
>;

pub struct Display<'a, I: DriverMode> {
    display: DisplayType<'a, I>,
    text_style: MonoTextStyle<'a, BinaryColor>,
    text_style_big: MonoTextStyle<'a, BinaryColor>,
}

impl<'a, I> Display<'a, I>
where
    I: DriverMode,
{
    pub fn init(i2c: I2c<'a, I>) -> Result<Display<'a, I>, DisplayError> {
        // Initialize display
        let interface = I2CDisplayInterface::new(i2c);
        let mut display = Ssd1306::new(interface, DisplaySize128x64, DisplayRotation::Rotate0)
            .into_buffered_graphics_mode();
        display.init()?;

        // Specify different text styles
        let text_style = MonoTextStyleBuilder::new()
            .font(&FONT_5X7)
            .text_color(BinaryColor::On)
            .build();

        let text_style_big = MonoTextStyleBuilder::new()
            .font(&FONT_9X18_BOLD)
            .text_color(BinaryColor::On)
            .build();

        Ok(Self {
            display,
            text_style,
            text_style_big,
        })
    }

    pub fn update(&mut self, state: State) -> Result<(), DisplayError> {
        let mut y = 9i32;
        let mut buf: heapless::String<64> = heapless::String::new();

        // title
        y = self.draw_title("Airflow", y)?;

        // Wifi
        let wifi_pos = Point::new(DIM_X as i32 - ICON_SIZE as i32 + V_SPACE, 0);
        match state.wifi {
            WifiStatus::Disconnected => {
                let wifi = WifiOff::new(BinaryColor::On);
                let image = Image::new(&wifi, wifi_pos);
                image.draw(&mut self.display)?;
            }
            WifiStatus::Connecting => {
                // TODO alternative for connecting status. animation?
                let wifi = WifiOff::new(BinaryColor::On);
                let image = Image::new(&wifi, wifi_pos);
                image.draw(&mut self.display)?;
            }
            WifiStatus::Connected => {
                let wifi = PrivateWifi::new(BinaryColor::On);
                let image = Image::new(&wifi, wifi_pos);
                image.draw(&mut self.display)?;
            }
        }

        // IP
        let ip_pos = Point::new(DIM_X as i32 - (ICON_SIZE * 2) as i32, 0);
        if state.ip.is_some() {
            let internet = Internet::new(BinaryColor::On);
            let image = Image::new(&internet, ip_pos);
            image.draw(&mut self.display)?;
        }

        // Time
        let dt = TIME_PROVIDER.get().now();
        write!(&mut buf, "{}", dt.format("%Y-%m-%d %H:%M:%S")).unwrap();
        _ = self.draw_text(&buf, y)?;
        buf.clear();

        self.display.flush()?;
        self.display.clear_buffer();
        Ok(())
    }

    pub fn draw_title(&mut self, text: &str, y: i32) -> Result<i32, DisplayError> {
        Text::with_alignment(text, Point::new(0, y), self.text_style_big, Alignment::Left)
            .draw(&mut self.display)?;
        Ok(y + V_SPACE + DELTA_Y)
    }

    pub fn draw_text(&mut self, text: &str, y: i32) -> Result<i32, DisplayError> {
        Text::with_alignment(text, Point::new(0, y), self.text_style, Alignment::Left)
            .draw(&mut self.display)?;
        Ok(y + V_SPACE + DELTA_Y)
    }
}
