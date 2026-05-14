# SCS PACTOR Transport

This crate provides transport implementations for SCS PACTOR-style links.

## USB Serial Smoke Test

The USB transport opens an SCS PACTOR modem such as a DR-7800, P4dragon, or
PTC-IIIusb as a serial device. The SCS DRAGON 7400/P4dragon USB interface uses
829440 baud, 8 data bits, no parity, 1 stop bit, and no flow control. That
829440 baud rate is the crate default because the hardware test targets the
DRAGON 7400 modems.

Example device paths:

- macOS: `/dev/tty.usbmodem*`
- Linux: `/dev/ttyUSB0`, or preferably `/dev/serial/by-id/usb-SCS_SCS_DRAGON_7400_<serial>-if00-port0`
- Windows: `COM3`, `COM4`, or the assigned modem COM port shown in Device Manager

Before running against hardware:

1. Connect the modem over USB and confirm the serial port path.
2. Put the HF rig on the intended frequency; this crate does not control CAT,
   tuning, or the auto-tuner.
3. Choose your station callsign for `MYCALL`.
4. Choose a known reachable PACTOR node callsign for `connect_peer`.

For repeatable Linux hardware tests, prefer the stable by-id paths because
`/dev/ttyUSB0` and `/dev/ttyUSB1` can swap after reconnects:

```text
cargo run -p bunker_coin_sim --bin pactor_hw_test -- \
  --port-a /dev/serial/by-id/usb-SCS_SCS_DRAGON_7400_DR83NDYP-if00-port0 \
  --port-b /dev/serial/by-id/usb-SCS_SCS_DRAGON_7400_DR752ZE5-if00-port0
```

The hardware test defaults to `--baud 829440`. If a modem has been configured
to a different USB serial rate in SCS tooling, pass it explicitly with
`--baud <rate>`.

Minimal smoke-test flow:

```rust
use scs_pactor::{PactorTransport, UsbPactorConfig, UsbPactorTransport};

# async fn smoke() -> Result<(), scs_pactor::ScsPactorError> {
let modem = UsbPactorTransport::connect(UsbPactorConfig::new("/dev/ttyACM0")).await?;
modem.set_mycall("N0CALL").await?;
modem.connect_peer("KNOWNNODE").await?;
modem.write_data(b"hello").await?;
modem.disconnect().await?;
# Ok(())
# }
```

Expected result: `connect_peer` returns after the modem reports
`CONNECTED KNOWNNODE`; `next_event` can be used to observe `CONNECTED`,
`DISCONNECTED`, `BUSY`, `QUEUED`, `LINK FAILURE`, and `LINK QUALITY` events.
