init:
	test -d ${KenshinVenv} || virtualenv ${KenshinVenv}

install: init
	@source ${KenshinVenv}/bin/activate; pip install -r requirements.txt
	@source ${KenshinVenv}/bin/activate; python setup.py install

restart_rurouni:
	source ${KenshinVenv}/bin/activate; python bin/kenshin-restart.py

restart_relay:
	find /service -name 'carbon-relay-*' | xargs -rL 1 svc -t

restart_web:
	find /service -name 'graphite-*' | xargs -rL 1 svc -t
