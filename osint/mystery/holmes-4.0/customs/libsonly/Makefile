# Stub Makefile for custom modules

extras:: $(LIBUCW)

ifdef CONFIG_CHARSET
extras:: $(LIBCHARSET)
endif

ifdef CONFIG_SHERLOCK_LIB
extras:: $(LIBSH)
endif

ifdef CONFIG_IMAGES
extras:: $(LIBIMAGES)
endif

ifdef CONFIG_LANG
extras:: $(LIBLANG)
endif

ifdef CONFIG_XML
extras:: $(LIBSHXML)
endif
