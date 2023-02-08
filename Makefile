# This is the Makefile helping you submit the labs.
# Just create 6.5840/api.key with your API key in it,
# and submit your lab with the following command:
#     $ make [lab1|lab2a|lab2b|lab2c|lab2d|lab3a|lab3b|lab4a|lab4b]

LABS=" lab1 lab2a lab2b lab2c lab2d lab3a lab3b lab4a lab4b "

%: check-%
	@echo "Preparing $@-handin.tar.gz"
	@if echo $(LABS) | grep -q " $@ " ; then \
		echo "Tarring up your submission..." ; \
		COPYFILE_DISABLE=1 tar cvzf $@-handin.tar.gz \
			"--exclude=src/main/pg-*.txt" \
			"--exclude=src/main/diskvd" \
			"--exclude=src/mapreduce/824-mrinput-*.txt" \
			"--exclude=src/mapreduce/5840-mrinput-*.txt" \
			"--exclude=src/main/mr-*" \
			"--exclude=mrtmp.*" \
			"--exclude=src/main/diff.out" \
			"--exclude=src/main/mrcoordinator" \
			"--exclude=src/main/mrsequential" \
			"--exclude=src/main/mrworker" \
			"--exclude=*.so" \
			Makefile src; \
		if test `stat -c "%s" "$@-handin.tar.gz" 2>/dev/null || stat -f "%z" "$@-handin.tar.gz"` -ge 20971520 ; then echo "File exceeds 20MB."; rm $@-handin.tar.gz; exit; fi; \
		echo "$@-handin.tar.gz successfully created. Please upload the tarball manually on Gradescope."; \
	else \
		echo "Bad target $@. Usage: make [$(LABS)]"; \
	fi

.PHONY: check-%
check-%:
	@echo "Checking that your submission builds correctly..."
	@./.check-build git://g.csail.mit.edu/6.5840-golabs-2023 $(patsubst check-%,%,$@)
