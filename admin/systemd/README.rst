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
* Error info: #  journalctl -xe

See also:

* Using Systemctl: https://www.digitalocean.com/community/tutorials/how-to-use-systemctl-to-manage-systemd-services-and-units
* Writing service files: http://patrakov.blogspot.com/2011/01/writing-systemd-service-files.html
* Log viewing: https://www.digitalocean.com/community/tutorials/how-to-use-journalctl-to-view-and-manipulate-systemd-logs