# Automatic conversion of docker images into the thin format

This utility will automatically convert normal docker images into the thin
format.

## Vocabulary

There are several concepts to keep track in this process, and none of them is
very common, so before to dive in we can agree on a shared vocabulary.

**Registry** does refer to the docker image registry, with protocol extensions,
common examples are:

    * https://registry.hub.docker.com
    * https://gitlab-registry.cern.ch

**Repository** This specifies a class of images, each image will be indexed,
then by tag or digest. Common examples are:

    * library/redis
    * library/ubuntu

**Tag** is a way to identify an image inside a repository, tags are mutable
and may change in a feature. Common examples are:

    * 4
    * 3-alpine

**Digest** is another way to identify images inside a repository, digests are
**immutable**, since they are the result of a hash function to the content of
the image. Thanks to this technique the images are content addressable.
Common examples are:

    * sha256:2aa24e8248d5c6483c99b6ce5e905040474c424965ec866f7decd87cb316b541
    * sha256:d582aa10c3355604d4133d6ff3530a35571bd95f97aadc5623355e66d92b6d2c


An **image** belongs to a repository -- which in turns belongs to a registry --
and it is identified by a tag, or a digest or both, if you can choose is always
better to identify the image using at least the digest.

To unique identify an image so we need to provide all those information:

    1. registry
    2. repository
    3. tag or digest or tag + digest

We will use slash (`/`) to separate the `registry` from the `repository` and
the colon (`/`) to separate the `repository` from the `tag` and the at (`@`) to
separate the `digest` from the tag or from the `repository`.

The final syntax will be:

    REGISTRY/REPOSITORY[:TAG][@DIGEST]

Examples of images are:
    * https://registry.hub.docker.com/library/redis:4
    * https://registry.hub.docker.com/minio/minio@sha256:b1e5dd4a7be831107822243a0675ceb5eabe124356a9815f2519fe02beb3f167
    * https://registry.hub.docker.com/wurstmeister/kafka:1.1.0@sha256:3a63b48894bce633fb2f0d2579e162163367113d79ea12ca296120e90952b463

## Concepts

The converter has a declarative approach. You specify what is your end goal and
it tries to reach it.

The main component of this approach is the **wish** which is a triplet
composed by the input image, the output image and in which cvmfs repository you
want to store the data.

    wish => (input_image, output_image, cvmfs_repository)

The input image in your wish should be as more specific as possible,
ideally specifying both the tag and the digest.

On the other end, you cannot be so specific for the output image, simple
because is impossible to know the digest before to generate the image itself.

Finally we model the repository as an append only structure, deleting
layers could break some images actually running.

## Recipes

Recipes are a way to describe the wish we are interested in convert.

### Recipe Syntax v1

An example of a complete recipe file is above, let's go over each key

``` yaml
version: 1
user: smosciat
cvmfs_repo: unpacked.cern.ch
output_format: '$(scheme)://registry.gitlab.cern.ch/thin/$(image)'
input:
        - 'https://registry.hub.docker.com/econtal/numpy-mkl:latest'
        - 'https://registry.hub.docker.com/agladstein/simprily:version1'
        - 'https://registry.hub.docker.com/library/fedora:latest'
        - 'https://registry.hub.docker.com/library/debian:stable'
```

**version**: indicate what version of recipe we are using, at the moment only
`1` is supported.
**user**: the user that will push the thin docker images into the registry,
the password must be stored in the `DUCC_OUTPUT_REGISTRY_PASS`
environment variable.
**cvmfs_repo**: in which CVMFS repository store the layers and the singularity
images.
**output_format**: how to name the thin images. It accepts few "variables" that
reference to the input image.

* $(scheme), the very first part of the image url, most likely `http` or `https`
* $(registry), in which registry the image is locate, in the case of the example it would be `registry.hub.docker.com`
* $(repository), the repository of the input image, so something like `library/ubuntu` or `atlas/athena`
* $(tag), the tag of the image examples could be `latest` or `stable` or `v0.1.4`
* $(image), the $(repository) plus the $(tag)

**input**: list of docker images to convert

This recipe format allow to specify only some wish, specifically all the images
need to be stored in the same CVMFS repository and have the same format.

## Commands

### convert

```
convert recipe.yaml
```

This command will try to convert all the wish in the recipe.

### loop

```
loop recipe.yaml
```

This command is equivalent to call `convert` in an infinite loop, useful to
make sure that all the images are up to date.

## convert workflow

The goal of convert is to actually create the thin images starting from the
regular one.

In order to convert we iterate for every wish in the recipe.

In general, some wish will be already converted while others will need to
be converted ex-novo.

The first step is then to check if the wish is already been converted.
In order to do this check, we download the input image manifest and check
in the repository if the specific image is been already converted, if it is we
safely skip such conversion.

Then, every image is made of different layers, some of them could already be
on the repository.
In order to avoid expensive CVMFS transaction, before to download and ingest
the layer we check if it is already in the repository, if it is we do not
download nor ingest the layer.

The conversion simply ingest every layer in an image, create a thin image and
finally push the thin image to the registry.

Such images can be used by docker with the  thin image plugins.

The daemon also transform the images into singularity images and store them
into the repository.

The layers are stored into the `.layer` subdirectory, while the singularity
images are stored in the `singularity` subdirectory.

## General workflow

This section explains how this utility is intended to be used.

Internally this utility invokes `cvmfs_server`, `docker` and `singularity`
commands, so it is necessary to use it in a stratum0 that also have docker
installed.

The conversion is quite straightforward, we first download the input image, we
store each layer on the cvmfs repository, we create the output image and unpack
the singularity one, finally we upload the output image to the registry.

It does not support downloading images that are not public.

In order to publish images to a registry is necessary to sign up in the
docker hub. It will use the user from the recipe, while it will read the
password from the `DUCC_OUTPUT_REGISTRY_PASS` environment variable.

## Run as daemon

DUCC provides an unit file suitable to be used by systemd. While used as a
daemon DUCC will run the `loop` command.

Environmental variables are used in order to provide input for DUCC. Two
variables need to be set:
1. `$DUCC_RECIPE_FILE`
2. `$DUCC_OUTPUT_REGISTRY_PASS`

The `$RECIPE_FILE` variable need to point to the recipe.yaml file that you want
to convert.

The `$DUCC_OUTPUT_REGISTRY_PASS` is the variable described
above. It needs to be set to the password of the docker registry that DUCC will
use to publish the docker images.

In order to set those variable, create an override file.

`systemctl edit cvmfs_ducc.service`

And then edit like the following:

```unit
[Service]
Environment="DUCC_RECIPE_FILE=UPDATE-ME.yaml"
Environment="DUCC_OUTPUT_REGISTRY_PASS=UPDATE-ME"
```
