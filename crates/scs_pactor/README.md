# SCS PACTOR Transport

This crate provides transport implementations for SCS PACTOR-style links.

## USB Serial Smoke Test

The USB transport opens a CDC-ACM serial device with 115200 baud, 8 data bits,
no parity, 1 stop bit, and no flow control by default.

Example device paths:

- macOS: `/dev/tty.usbmodem*`
- Linux: `/dev/ttyACM0` or `/dev/ttyUSB0`
- Windows: `COM3`, `COM4`, or the assigned modem COM port shown in Device Manager

Minimal usage:

```rust
use scs_pactor::{PactorTransport, UsbPactorConfig, UsbPactorTransport};

# async fn smoke() -> Result<(), scs_pactor::ScsPactorError> {
let modem = UsbPactorTransport::connect(UsbPactorConfig::new("/dev/ttyACM0")).await?;
modem.set_mycall("N0CALL").await?;
modem.connect_peer("REMOTE").await?;
modem.write_data(b"hello").await?;
modem.disconnect().await?;
# Ok(())
# }
```

Before running against hardware, put the radio on the intended frequency and
confirm the modem is visible as a serial device.
