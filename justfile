# Build for running in Wokwi simulator
wokwi-build:
    @cargo build --profile=wokwi --features=wokwi

# Launch the Wokwi gateway
wokwi-gateway:
    @wokwigw
