from docker_injector import DockerInjector



injector = DockerInjector("localhost:443", "heposlibs", "latest")
injector.setup("cvmfs")
