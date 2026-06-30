# Vault Bootstrap Notes

## Scope

Vault was deployed as the Aegis secret management foundation in `aegis-security`.

## Current Mode

Vault is running in standalone mode with persistent storage backed by `nutanix-volume`.

## Current Seal Mode

Vault uses Shamir manual unseal.

If the Vault pod restarts, it must be unsealed manually using the saved unseal key.

## Current Secret Engine

A KV v2 secret engine is enabled at:

```text
aegis/
