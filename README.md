Kenshin
=============

> Kenshin ([るろうに剣心](http://zh.wikipedia.org/wiki/%E6%B5%AA%E5%AE%A2%E5%89%91%E5%BF%83))

Kenshin project consists of two major components:

- `Kenshin` is a fixed-size time-series database format, similar in design to [Whisper](https://github.com/graphite-project/whisper), it's an alternative to Whisper for [Graphite](https://github.com/graphite-project) storage component. Whisper performs lots of tiny I/O operations on lots of different files, Kenshin is aiming to improve the I/O performance. For more design details please refer to [design docs](https://github.com/douban/Kenshin/wiki/design) (Chinese) and QCon 2016 Presentation [slide](https://github.com/zzl0/zzl0.github.com/raw/master/files/QCon-Kenshin.pdf).

- `Rurouni-cache` is a storage agent that sits in front of kenshin to batch up writes to files to make them more sequential, rurouni-cache is to kenshin as carbon-cache is to whisper.

Kenshin is developing and maintaining by Douban Inc. Currently, it is working in production environment, powering all metrics (host, service, DAE app, user defined) in douban.com. In our environment, after using Kenshin, the IOPS is decreased by 97.5%.

Quick Start
------------------

We recommend using virtualenv when installing dependencies:

    $ virtualenv venv
    $ source venv/bin/activate
    $ pip install -r requirements.txt

Tests can be run using nosetests:

    $ nosetests -v

Setup configuration

    $ misc/init_setup_demo.sh

Start Kenshin instance
    
    $ python bin/rurouni-cache.py --debug --config=conf/rurouni.conf --instance=0 start

Send metrics to Kenshin instance

    $ python examples/rurouni-pickle-client.py 1

Query data in cache

    $ python bin/kenshin-cache-query.py system.loadavg.min_1.metric_test

Query data in file
    
    $ python bin//kenshin-fetch.py storage/link/0/system/loadavg/min_1/metric_test.hs --from <timestamp>

Get kenshin file info

    $ python bin/kenshin-info.py storage/link/0/system/loadavg/min_1/metric_test.hs


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


### What's the performance of Kenshin?

In our environment, the IOPS is decreased by 97.5%, and the query latency is not significantly slower than Whisper.
<img src="/img/kenshin-perf.png" width="400"/>


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