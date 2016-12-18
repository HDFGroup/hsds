######################
Systemd Setup
######################

This directory contains systemd service files for running service and data nodes as and
systemd service.

They need to be copied (or linked) to the systemd system directory:
/etc/systemd/system.

Use the following commands to manage the services (using either hsds_sn or hsds_sn):

* Start service: # systemctl start hsds_dn 
* Get Status: # systemctl status hsds_dn
* View log: # journalctl -u hsds_dn.service
* Reload service (say if service file is changed): # systemctl daemon-reload
* Stop service: # systemctl stop hsds_dn