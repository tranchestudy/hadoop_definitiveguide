When developing Hadoop applications, it is common to switch between
running the application locally and running it on a cluster. One way
to accommodate these variations is to have Hadoop configuration files
containing the connection settings for each cluster you run against,
and specify which one you are using when you run Hadoop applications
or tools. As a matter of best practice, it's recommended to keep these
files outside Hadoop's installation directory tree, as this makes it
easy to switch between Hadoop versions without duplicating or losing
settings.