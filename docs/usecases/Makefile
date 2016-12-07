
#
# Builds a basic pdf of the use case docs. rst2pdf is required for this to work. 
#

RSTFLS= into.rst extract_one_dimensional_subset.rst extract_two_dimensional_subset.rst \
		  extract_three_dimensional_subset.rst extract_several_one_dimensional_subsets.rst \
		  extract_subsets_with_shared_common_dimensions.rst view_end-point_metadata.rst

TARGET=usecases.rst

all: clean rst pdf 

rst:
	@$(foreach f, ${RSTFLS}, cat ${f} >> ${TARGET}; echo "\n---------\n" >> ${TARGET} ;)

pdf:
	rst2pdf -v ${TARGET}

clean:
	rm -f ${TARGET} *.pdf
