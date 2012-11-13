export GIT_DIR := $(GIT_CLONE_DIR)/.git


pre-build::
	export BUILD_DIR=$(shell pwd); \
	export PKG_VERSION=$(shell parsechangelog | perl -ne'if(m/^Version: .+\.(.+?)(?:-.+)?$$/){print "$$1\n";}'); \
	if test -f git_ls_files; then \
		echo "pre-build was ran previously"; \
	else \
		git clone $(GIT_REPO) $(GIT_CLONE_DIR); \
		cd $(GIT_CLONE_DIR); \
		git checkout $(GIT_BRANCH);\
		git ls-files > $$BUILD_DIR/git_ls_files; \
		cd $$BUILD_DIR; \
	fi

clean::
	#cat git_ls_files | xargs rm -rf
	rm -rf $(CLEAN_FILES)
	rm -rf git_ls_files
	rm -rf $(GIT_CLONE_DIR)
