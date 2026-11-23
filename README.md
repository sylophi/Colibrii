# Colibrii

Colibrii connects a person's devices to each other, for any app. It
introduces two of the person's devices, sets up an encrypted connection
between them, and gets out of the way: the app's data goes from device
to device, and Colibrii never reads it.

It has two halves:

- **The service** keeps each app's devices, says which are online, and
  sets up the tunnels that let a device be reached from outside its
  network. Whoever runs an app runs their own instance of it, on their
  own Cloudflare account and domains.
- **An SDK** in the app enrolls the device, follows the device list,
  dials and listens, and runs the tunnel. Desktop apps, programs on a
  server, and web pages can all use it.

People sign in with [Clerk](https://clerk.com). Each app brings its own
Clerk instance, and Colibrii has none of its own.

## Status

The wire is specified in [spec/](spec/README.md). The service is in
[service/](service/README.md), with its setup. The TypeScript SDK in
[sdk/ts](sdk/ts/README.md) holds the wire (the schemas the service and
the SDK share, Noise, the relay's sealing, and the peer link's
handshake and messages) and the device runtime, with its bindings for
Node and for web pages. The test vectors in `spec/vectors/` cover the
wire.
