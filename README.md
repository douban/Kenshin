Kenshin
=============

[![travis-ci status](https://travis-ci.org/douban/Kenshin.svg)](https://travis-ci.org/douban/Kenshin)

> Kenshin ([るろうに剣心](http://zh.wikipedia.org/wiki/%E6%B5%AA%E5%AE%A2%E5%89%91%E5%BF%83))

Kenshin project consists of two major components:

- `Kenshin` is a fixed-size time-series database format, similar in design to [Whisper](https://github.com/graphite-project/whisper), it's an alternative to Whisper for [Graphite](https://github.com/graphite-project) storage component. Whisper performs lots of tiny I/O operations on lots of different files, Kenshin is aiming to improve the I/O performance. For more design details please refer to [design docs](https://github.com/douban/Kenshin/wiki/design) (Chinese) and QCon 2016 Presentation [slide](https://github.com/zzl0/zzl0.github.com/raw/master/files/QCon-Kenshin.pdf).

- `Rurouni-cache` is a storage agent that sits in front of kenshin to batch up writes to files to make them more sequential, rurouni-cache is to kenshin as carbon-cache is to whisper.

Kenshin is developing and maintaining by Douban Inc. Currently, it is working in production environment, powering all metrics (host, service, DAE app, user defined) in douban.com.


### What's the performance of Kenshin?


In our environment, after using Kenshin, the IOPS is decreased by 97.5%, and the query latency is not significantly slower than Whisper.

<img src="/img/kenshin-perf.png" width="400"/>


Quick Start
------------------

We recommend using virtualenv when installing dependencies:

    $ git clone https://github.com/douban/Kenshin.git
    $ cd Kenshin
    $ virtualenv venv
    $ source venv/bin/activate
    $ pip install -r requirements.txt

Tests can be run using nosetests:

    $ nosetests -v

Setup configuration

    $ misc/init_setup_demo.sh

Setup Kenshin

    $ python setup.py build_ext --inplace && python setup.py install

Start two rurouni-cache instances

    $ python bin/rurouni-cache.py --debug --config=conf/rurouni.conf --instance=0 start
    $ python bin/rurouni-cache.py --debug --config=conf/rurouni.conf --instance=1 start

Then go to [Graphite-Kenshin](https://github.com/douban/graphite-kenshin) for starting Web instances.

FAQ
----------


### Why don't you just use whisper?

Whisper is great, and initially we did use it. Over time though, we ran into several issues:

1. Whisper using a lot of IO. There are serval reasons:
    - Using one file per metric.
    - Realtime downsample feature (different data resolutions based on age) causes a lot of extra IO
2. Carbon-cache & Carbon-relay is inefficient and even is cpu-bound. We didn't write our own carbon-relay, but replaced carbon-relay with [carbon-c-relay](https://github.com/grobian/carbon-c-relay).


### Why did you totally rewrite whisper? Couldn't you just submit a patch?

The reason I didn't simply submit a patch for Whisper is that kenshin's design is incompatible with Whisper's design. Whisper using one file per metric. Kenshin on the other hand merge N metrics into one file.

### How to intergrate Kenshin with Graphite-Web?

We use write a plugin for Graphite-API named [Graphite-Kenshin](https://github.com/douban/graphite-kenshin)

Acknowledgments
------------------

- Thanks for the [Graphite](https://github.com/graphite-project) project.
- Thanks to [@grobian](https://github.com/grobian) for the [carbon-c-relay](https://github.com/grobian/carbon-c-relay) project.


Contributors
---------------

- [@zzl0](https://github.com/zzl0)
- [@mckelvin](https://github.com/mckelvin)
- [@windreamer](https://github.com/windreamer)
- [@youngsofun](https://github.com/youngsofun)


License
-------

Kenshin is licensed under version 2.0 of the Apache License. See the [LICENSE](/LICENSE) file for details.
