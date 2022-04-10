/*
 *	Sherlock Content Analyser -- List of modules
 *
 *	(included by analyser/analyser.[ch])
 */

AN_MODULE(an_test)
AN_MODULE(an_ip_ranges)
AN_MODULE(an_substr)

#ifdef CONFIG_LANG
AN_MODULE(an_lang)
#endif

#if defined(CONFIG_IMAGES_DUP) || defined(CONFIG_IMAGES_SIM)
AN_MODULE(an_imagesig)
#endif

#ifdef CUSTOM_ANALYSERS
CUSTOM_ANALYSERS
#endif
