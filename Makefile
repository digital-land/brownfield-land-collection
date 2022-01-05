PIPELINE_FLAGS=--save-harmonised

include makerules/makerules.mk
include makerules/development.mk
include makerules/collection.mk

dataset::
	mkdir -p $(HARMONISED_DIR)brownfield-land

include makerules/pipeline.mk

clobber::
	rm -rf $(HARMONISED_DIR)
