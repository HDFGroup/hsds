Socket Performance Test
=======================

This folder contains a stand-alone test for socket performance using Python.

Run: `python server.py` to run the socket writer.  THe script will block till `python client.py` is executed to 
read from the socket.  Use the "connecting to: ..." value from server.py as the
argument to client.py.

Example:

    $ python server.py
    connecting to: 127.0.0.1:58489
    # server blocks here waiting for client.py

In a seperate shell:

    $ python client.py 127.0.0.1:58489
    bytes: 1048576
    Elapsed time ::  0.001 s, 1453.33 Mb/s

In the first shell, server.py now completes:

    sending 1048576
    1048576 bytes sent
    done

Configuration values in config.py can be altered to:

    * change host or use a specfic port
    * Use Unix domain sockets rather than TCP sockets (for local host only)
    * Use shared memory to transfer data (for local host only)
    * Change the number of bytes sent

Rather than modifying the configuration.py file, the config values can be
sepecified in the command line or via environment variables.

E.g.: `python server.py --num_bytes=104857600  # send 100MB of data`

    
