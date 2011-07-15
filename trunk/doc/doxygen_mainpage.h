/**
 * \mainpage CernVM File System
 * 
 * \section About About CVMFS
 * The file system optimized for software distribution (CMVFS) plays the key role in the “thin appliance” model used by CernVM. 
 * It is implemented as File System in User Space (FUSE) module. 
 * CVMFS has been designed to make a directory tree stored on a Web server look like a local read-only file system on the virtual machine. 
 * On the client side in requires only an outgoing HTTP/HTTPS connectivity to the Web server.  
 * Assuming that the files are read only, CVMFS does an aggressive file level caching. 
 * Both files and file metadata are cached on local disk as well as cached on shared proxy servers, allowing the file system to scale to a very large number of clients. 
 *
 * The procedures for building, installing and validating software release remains in the hand of, and the responsibility of, each user community. 
 * To each community we provide an instance of CernVM that runs CVMFS in read/write mode allowing experiment software manager to install, test and publish configured experiment software releases to our central distribution point. 
 * At present all four LHC experiments are supported, and already distribute about 100 GB of their software using this method. 
 * Adding another user community is a straight forward, simple and scalable procedure since all users may but do not necessarily have to share the same central software distribution point (HTTP server).
 *
 * \image html release.jpg "Software releases are first uploaded by the experiment’s release manager to a dedicated virtual machine, tested and then published on a Web server acting as central software distribution point."
 *
 * \section Arch Architectural Notes
 * First implementation of CVMFS was based on GROW-FS file system which was originally provided as one of private file system options available in Parrot. 
 * Parrot traps the system I/O calls and that is resulting in a performance penalty and occasional compatibility problems with some applications. 
 * Since in the virtual machine we have full access to the kernel and can deploy our own kernel modules, we have constructed CVMFS from modified Parrot and GROW-FS code base and adapted them to run as a FUSE kernel module adding in the process some new features to improve scalability, performance and simplify deployment. 
 * The principal new features in CVMFS compared to GROW-FS are:
 *   - Using FUSE kernel module allows for in-kernel caching of file attributes.
 *   - Capability to work in offline mode providing that all required files are cached. 
 *   - Possibility to use the hierarchy of file catalogues on the server side.
 *   - Transparent file compression/decompression.
 *   - Dynamical expansion of environment variables embedded in symbolic links.
 *
 * In the current deployment model, we envisage a hierarchy of cache servers to be deployed between a client and the central server allowing the delivery network to be organized as a fine-grained distribution tree. 
 * Proxy servers may serve CernVM clients as well as other proxy servers. 
 * In this model each proxy server stores the union set of required data of its sub tree. 
 * By adding and removing proxy servers, the tree can be adapted to respond to required load.
 *
 * At present, such a deployment model is fully supported and CernVM will always use the nearest proxy server as pointed to by the configuration service at start up time. 
 * In spite of reasonable flexibility, this approach is still static in nature, requires additional manual effort to setup and maintain the proxy hierarchy and clearly has a single point of failure.
 * To remove a single point of failure we can exploit an existing commercial infrastructure like Amazon's CloudFront service. 
 * If we mirror our repository to Amazon Simple Storage Service (S3), CloudFront Service will assure that users access the content using some nearby mirror. 
 * This is done by assigning a canonical DNS name to an S3 resource, by which the client transparently connects to the closest mirror. 
 * Mirroring is done on demand and expired content will be deleted from mirror servers automatically. 
 * An alternative to CloudFront that works on similar principles is the Coral Content Distribution Network. 
 *
 * If we consider scenario where a single site runs many CernVM instances we may still run into a local bandwidth bottleneck if every CernVM client is independently fetching data from content delivery network. 
 * To tackle that problem, we are adding a capability to CernVM to discover other CernVM instances on a local network using Avahi system for service discovery. 
 * Since every CernVM instance keeps a local cache of files it has already downloaded, it can also share this cache with neighbours acting as an ad-hoc proxy. 
 * This way we can achieve a self-organizing network on a LAN level in a manner similar to peer-to-peer networks while retaining most of the existing CernVM components and accessing top level repository residing on content distributions networks such as CloudFront or Coral, possibly via a chain of additional proxy caches. 
 * This combined model has all of the ingredients necessary to assure scalability to support thousands of CernVM clients with the reliability expected from a file system, and performance matching that of traditional network file systems (such as NFS or AFS) on a local network.
 *
 * \section Blocks Block Diagram
 * \image html cvmfs-blocks.jpg
 * \image latex cvmfs-blocks.jpg
 */

/* Figure 2: Software releases are first uploaded by the experiment’s release manager to a dedicated virtual machine, tested and then published on a Web server acting as central software distribution point.
Figure 3: In the current software version, CernVM clients fetch the data files from central repository via the hierarchy of proxy servers (a) as specified by the configuration file obtained from the configuration server at boot time.  We plan to remove a single point of failure using content distribution network on WAN and self organizing P2P-like cache sharing on LAN (b). 
*/