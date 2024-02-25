Docker setup instructions
=========================

The following are instructions for installing Docker on Linux/Ubuntu.
Details for other Linux distros may vary. See
https://docs.docker.com/get-docker/ for more detailed instructions.

Run the following commands to install Docker on Linux/Ubuntu:

1. ``sudo apt-get update``
2. ``sudo apt install docker.io``
3. ``sudo systemctl start docker``
4. ``sudo systemctl enable docker``
5. ``sudo groupadd docker`` if group docker doesn’t exist already
6. ``sudo gpasswd -a $USER docker``
7. Log out and back in again (you may also need to stop/start docker
   service)
8. ``docker ps`` to verify that Docker is running.

Install docker-compose:

1. See: https://docs.docker.com/compose/install/
