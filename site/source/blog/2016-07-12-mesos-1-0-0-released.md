---
layout: post
title: Apache Mesos 1.0.0 Released
permalink: /blog/mesos-1-0-0-released/
published: true
post_author:
  display_name: Vinod Kone
  twitter: vinodkone
tags: Release
---

Mesos 1.0 release is finally here for[download](http://mesos.apache.org/downloads)! This is the biggest release of Mesos yet, packed with tons of new features and stability improvements.

## Why 1.0

As might already know, Mesos has been running in production in some of the largest webscale [companies]() like [Twitter](http://twitter.com) and [Apple](http://apple.com) for years now. It has proven to scale to tens of thousands of nodes and hundreds of thousands of containers serving live traffic. For example, Apple's Siri runs on Mesos clusters!

Given its history, Apache Mesos could've reached the 1.0 milestone a long time ago. But the one thing we really wanted to improve before doing a 1.0 release was new HTTP APIs for frameworks and operators. The new APIs make it really easy to write a framework or develop a tool in a language of your choice with off the shelf HTTP libraries. After iterating on the new APIs for nearly an year, we are confident that the new APIs are ready for widespread use. As part of the new APIs we've also versioned the APIs and formulated our release and support policy. Given these milestones we are happy to call our latest release 1.0.

This release also includes the following features:

### CNI support

### GPU support

### Fine-grained Authorization

### Windows support


Furthermore, several bugfixes and improvements made it into this release.
For full release notes with all features and bug fixes, please refer to the [CHANGELOG](https://git-wip-us.apache.org/repos/asf?p=mesos.git;a=blob_plain;f=CHANGELOG;hb=0.28.0).

### Upgrades

Rolling upgrades from a Mesos 0.28.x cluster to Mesos 1.0.0 are straightforward. There are just some minor, backwards compatible deprecations.
Please refer to the [upgrade guide](http://mesos.apache.org/documentation/latest/upgrades/) for detailed information on upgrading to Mesos 1.0.0.


### Try it out

We encourage you to try out this release and let us know what you think.
If you run into any issues, please let us know on the [mailing lists, Slack Channel or IRC](https://mesos.apache.org/community).

### Thanks!

Thanks to the ** contributors who made 1.0.0 possible:

