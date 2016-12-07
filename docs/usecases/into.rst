HSDS Use Cases 
==============

Overview
------------
The following document series summarizes use cases for the Highly Scalable Data Service (HSDS) project [#]_, 
NASA ACCESS award. The proposed work under the HSDS project strives to developed a highly scalable 
web service to vend NASA earth science data. The HSDS system is built upon Object Storage [#]_ rather than 
traditional POSIX [#]_ file system based storage that is in common use today. The proposed work is to be developed 
on Amazon Web Services (AWS) Simple Storage Service (S3) [#]_. Though the system will use AWS S3 initially, it 
may be ported to other object storage providers such as Microsoft Azure [#]_, Google Cloud [#]_, and potentially be 
deployable on private clouds systems, such as Open Stack [#]_. The use of cloud based infrastructure may offer 
several key benefits of scalability, built in redundancy and reduced total cost of ownership as compared 
with a traditional data center approach. However, most of the tools and software systems developed for 
NASA data repositories were not developed with a cloud based infrastructure in mind and do not fully 
take advantage of commonly available cloud-based technologies. To enable compatibility with existing 
tools and applications, we outline client libraries that are API compatible with existing libraries for 
HDF5 [#]_ and NetCDF4 [#]_.

Goals
-----
The core goals of the HSDS project is to implement and enable agencies to deploy a data service that 
significantly improves fine grain data distribution scale-out capabilities with minimal effort using 
object store systems. The service shall have the capabilities to serve many simultaneously users 
from a near infinite and diverse set of locations throughout the Internet. The service shall have the 
capabilities to serve many simultaneously users from within a private clouds setting. Any configuration
adaptations between the public and private cloud object store systems shall be minimal. The API interface  
shall be near API compatible with the existing HDF5 libraries that many users and systems are familiar with.

HSDS key differentiators
-------------------------
An object store based data service often shines when used with low cost commodity hardware and or a service 
completely managed by cloud service providers such as AWS. With HSDS deployed on a cloud service, data 
providers are often free from planning, managing, maintaining and securing physical storage appliances and 
may inherit scalable storage systems under several compliance requirements with minimal effort. 

The system will not attempt to directly compete with common High Performance Compute (HPC) center file 
system deployments, e.g. parallel POSIX like file systems such as Lustre [#]_ that is interconnected 
to a large array of compute nodes through high speed InfiniBand [#]_. HSDS will provide and alternative 
way to provide data at scale across a board range of system environments that is not possible currently 
with Lustre, gluster or nfs deployments. 

The HSDS service is easy to deploy and scale automatically. The service may employ an autoscaling
service based on immediate usage needs and data provider budgets. Service administrators need only set 
scaling parameters, e.g. maximum scale-out, minimum availability, etc... giving data providers the 
ability to tune performance vs costs in near relative with little effort. The test cloud service 
provider (AWS) currently has this functionality available at low cost [#]_. 


Terms and definitions
----------------------
For the use cases listed here, the terms *reasonable* and *end-point* are often used. We loosely define the 
term *reasonable* as an average http request timeout interval but the term can take on different meanings based
on the application context. The term *end-point* is used to note an access point at a given hyperplink, e.g.

::

   https://data.hdfgroup.org/tasmax_day_1950-2100.data.hdfgroup.org


.. [#] https://github.com/HDFGroup/hsds
.. [#] https://en.wikipedia.org/wiki/Object_storage
.. [#] https://en.wikipedia.org/wiki/POSIX
.. [#] https://aws.amazon.com/s3/
.. [#] https://azure.microsoft.com/en-us/ 
.. [#] https://cloud.google.com/
.. [#] https://www.openstack.org
.. [#] https://support.hdfgroup.org/HDF5/
.. [#] http://www.unidata.ucar.edu/software/netcdf/  
.. [#] http://lustre.org 
.. [#] https://www.openfabrics.org
.. [#] https://aws.amazon.com/autoscaling/

