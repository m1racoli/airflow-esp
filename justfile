# Build for running in Wokwi simulator
wokwi-build:
    @cargo build --profile=wokwi --features=wokwi

# Launch the Wokwi gateway
wokwi-gateway:
    @wokwigw

# run just commands from the dev project
dev *ARGS:
    @just dev/{{ARGS}}
