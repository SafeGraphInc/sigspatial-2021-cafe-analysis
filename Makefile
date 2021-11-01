setup:
	./setup.sh

clean:
	rm -rf apache-sedona-1.1.0-incubating-bin spark-3.1.2-bin-hadoop2.7 venv

distclean: clean
	rm -rf *.tgz *.tar.gz data venv data *.tbz2 *.jar
