# SCS PACTOR Transport

This crate provides transport implementations for SCS PACTOR-style links.

## USB Serial Smoke Test

The USB transport opens an SCS PACTOR modem such as a DR-7800, P4dragon, or
PTC-IIIusb as a CDC-ACM serial device. Defaults are 115200 baud, 8 data bits,
no parity, 1 stop bit, and no flow control.

Example device paths:

- macOS: `/dev/tty.usbmodem*`
- Linux: `/dev/ttyACM0` or `/dev/ttyUSB0`
- Windows: `COM3`, `COM4`, or the assigned modem COM port shown in Device Manager

Before running against hardware:

1. Connect the modem over USB and confirm the serial port path.
2. Put the HF rig on the intended frequency; this crate does not control CAT,
   tuning, or the auto-tuner.
3. Choose your station callsign for `MYCALL`.
4. Choose a known reachable PACTOR node callsign for `connect_peer`.

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
