# Web App Network Accessibility & DNS Configuration Guide

This document explains how to configure network routing, firewalls, and DNS resolution to ensure that the QP2 Web Application (deployed as described in the [Single-Machine Deployment Guide](single-machine-install.md)) is accessible from both internal (in-facing) and external (outfacing) networks.

---

## 1. The Core Constraint: Always Use the Domain Name

**Never access the portal using a bare IP address** (e.g., `https://<INTERNAL_IP>/data_portal/` or `https://<EXTERNAL_NAT_IP>/data_portal/`).
* **TLS Certificates:** The SSL/TLS certificate (`*.aps.anl.gov`) is bound to the domain name `bl2ws8-gmca.aps.anl.gov`. Accessing by IP will cause browser security warnings and block connections.
* **CORS Validation:** The backend uses `QP2_CORS_ORIGINS=https://bl2ws8-gmca.aps.anl.gov` for security. Requests initiated from an IP-based URL will be rejected by the backend's CORS policy.

---

## 2. Network-Level Solutions (DNS & Firewall)

Since clients must use the domain name, you must ensure that the domain name resolves to the correct IP address depending on where the client is located.

### Option A: Split-Horizon DNS (Recommended Permanent Fix)
Configure your network's DNS servers so that resolution is context-aware:
* **Internal (On-Site) Clients:** The internal DNS server should resolve `bl2ws8-gmca.aps.anl.gov` directly to the in-facing IP:
  ```text
  bl2ws8-gmca.aps.anl.gov  --->  <INTERNAL_IP>
  ```
* **External (Public Internet) Clients:** The public DNS server should resolve `bl2ws8-gmca.aps.anl.gov` to the external outfacing NAT IP:
  ```text
  bl2ws8-gmca.aps.anl.gov  --->  <EXTERNAL_NAT_IP>
  ```
This prevents internal clients from having their traffic routed out to the border firewall and reflected back, optimizing performance and security.

### Option B: NAT Hairpinning / Reflection (Firewall Fix)
If you cannot configure split-horizon DNS, the border firewall must be configured to support **NAT Hairpinning** (NAT Loopback / NAT Reflection). 
* When an internal client requests `bl2ws8-gmca.aps.anl.gov`, it resolves to the external IP `<EXTERNAL_NAT_IP>`.
* The firewall must recognize that this external IP belongs to a local resource and route/reflect the packets back internally to `<INTERNAL_IP>` instead of dropping them.

---

## 3. Local/Developer Workarounds

If you are testing or configuring individual client machines before DNS or firewall changes are fully implemented:

### A. Edit `/etc/hosts` (On individual internal machines or the server itself)
You can manually force resolution to bypass external DNS by adding an entry in `/etc/hosts` (on Linux/macOS) or `C:\Windows\System32\drivers\etc\hosts` (on Windows):
```text
# Map the domain directly to the internal IP
<INTERNAL_IP>  bl2ws8-gmca.aps.anl.gov
```
For the server itself (`bl2ws8`), map it to loopback to test completely locally:
```text
127.0.0.1  bl2ws8-gmca.aps.anl.gov
```

### B. Use `curl --resolve` for CLI Verification
To verify the public interface from the server itself without hairpin NAT issues, use `curl`'s `--resolve` parameter to bypass DNS queries:
```bash
curl --resolve bl2ws8-gmca.aps.anl.gov:443:127.0.0.1 -sI https://bl2ws8-gmca.aps.anl.gov/data_portal/
```

---

## 4. Server-Side Configurations (Why no changes are needed here)

The deployment configuration in the guide is already set up to accommodate both networks:
* **Apache Bind:** The VirtualHost directive `<VirtualHost *:443>` instructs Apache to listen on port 443 on **all available network interfaces** (both loopback, internal `<INTERNAL_IP>`, and any other interface).
* **Reverse Proxy:** Apache proxies requests internally to `127.0.0.1:8000`. Because this proxy action happens locally on the loopback interface, it remains secure and unaffected by which interface the external traffic arrived on.
