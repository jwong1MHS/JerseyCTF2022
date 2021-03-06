/*
 *	Sherlock Library -- Database Manager -- Tests and Benchmarks
 *
 *	(c) 1999 Martin Mares <mj@ucw.cz>
 *
 *	This software may be freely distributed and used according to the terms
 *	of the GNU Lesser General Public License.
 */

#if 1
#include "sherlock/db.c"
#define NAME "SDBM"
#else
#include "sherlock/db-emul.c"
#define NAME "GDBM"
#endif

#include <stdlib.h>
#include <unistd.h>
#include <stdarg.h>
#include <sys/stat.h>

static struct sdbm_options opts = {
  flags: SDBM_CREAT | SDBM_WRITE,
  name: "db.test",
  page_order: 10,
  cache_size: 1024,
  key_size: -1,
  val_size: -1
};

static struct sdbm *d;
static int key_min, key_max;		/* min<0 -> URL distribution */
static int val_min, val_max;
static int num_keys;			/* Number of distinct keys */
static int verbose;

static void
help(void)
{
  printf("Usage: dbtest [<options>] <commands>\n\
\n\
Options:\n\
-c<n>		Use cache of <n> pages\n\
-p<n>		Use pages of order <n>\n\
-k<n>		Use key size <n>\n\
-k<m>-<n>	Use key size uniformly distributed between <m> and <n>\n\
-kU		Use keys with URL distribution\n\
-n<n>		Number of distinct keys\n\
-d<m>[-<n>]	Use specified value size (see -k<m>-<n>)\n\
-t		Perform the tests on an existing database file\n\
-v		Be verbose\n\
-s		Turn on synchronous mode\n\
-S		Turn on supersynchronous mode\n\
-F		Turn on fast mode\n\
\n\
Commands:\n\
c		Fill database\n\
r		Rewrite database\n\
f[<p>%%][<n>]	Find <n> records with probability of success <p>%% (default=100)\n\
F[<p>%%][<n>]	Find, but don't fetch values\n\
d		Delete records\n\
w		Walk database\n\
W		Walk, but don't fetch values\n\
");
  exit(0);
}

static uns
krand(uns kn)
{
  return kn * 2000000011;
}

static uns
gen_url_size(uns rnd)
{
  uns l, m, r;
  static uns utable[] = {
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 3, 22, 108, 245, 481, 979, 3992, 7648, 13110, 19946, 27256, 34993, 43222, 52859, 64563,
80626, 117521, 147685, 188364, 233174, 290177, 347132, 407231, 465787, 540931, 628601, 710246, 808671, 922737, 1025691, 1138303,
1238802, 1344390, 1443843, 1533207, 1636494, 1739082, 1826911, 1910725, 1993940, 2094365, 2188987, 2267827, 2350190, 2441980,
2520713, 2593654, 2668632, 2736009, 2808356, 2889682, 2959300, 3017945, 3086488, 3146032, 3204818, 3251897, 3307001, 3349388,
3392798, 3433429, 3476765, 3529107, 3556884, 3585120, 3633005, 3677697, 3699561, 3716660, 3739823, 3765154, 3795096, 3821184,
3858117, 3908757, 3929095, 3943264, 3957033, 3969588, 3983441, 3994630, 4005413, 4028890, 4039678, 4058007, 4071906, 4087029,
4094233, 4105259, 4111603, 4120338, 4127364, 4133983, 4140310, 4144843, 4150565, 4155974, 4165132, 4170648, 4176811, 4187118,
4190866, 4199051, 4206686, 4216122, 4226109, 4233721, 4254123, 4261792, 4270396, 4276650, 4282932, 4291738, 4295932, 4299370,
4304011, 4307098, 4311866, 4318168, 4325730, 4329774, 4332946, 4336305, 4339770, 4345237, 4349038, 4356129, 4362872, 4366542,
4371077, 4374524, 4376733, 4378794, 4380652, 4382340, 4383552, 4385952, 4386914, 4393123, 4394106, 4395142, 4396593, 4399112,
4399909, 4401015, 4401780, 4402616, 4403454, 4404481, 4405231, 4405947, 4406886, 4408364, 4409159, 4409982, 4410872, 4412010,
4413341, 4414161, 4415673, 4417135, 4418032, 4419117, 4419952, 4420677, 4421387, 4421940, 4422469, 4423210, 4423696, 4424274,
4424982, 4425665, 4426363, 4427018, 4427969, 4428992, 4429791, 4430804, 4432601, 4433440, 4434157, 4434967, 4436280, 4439784,
4444255, 4445544, 4446416, 4447620, 4449638, 4453004, 4455470, 4456982, 4457956, 4458617, 4459538, 4460007, 4460377, 4460768,
4461291, 4461520, 4461678, 4461911, 4462063, 4462239, 4462405, 4462607, 4462666, 4462801, 4462919, 4463108, 4463230, 4463438,
4463530, 4463698, 4463779, 4463908, 4463991, 4464138, 4464188, 4464391, 4464580, 4464868, 4464980, 4465174, 4465255, 4465473,
4465529, 4465681, 4465746, 4465916, 4465983, 4466171, 4466248, 4466430, 4466560, 4466751, 4466930, 4467807, 4468847, 4469940,
4470344, 4470662, 4470716, 4471120, 4471389, 4471814, 4472141, 4472545, 4472687, 4473051, 4473253, 4473603, 4473757, 4474065,
4474125, 4474354, 4474428, 4474655, 4474705, 4474841, 4474858, 4475133, 4475201, 4475327, 4475367, 4475482, 4475533, 4475576,
4475586, 4475616, 4475637, 4475659, 4475696, 4475736, 4475775, 4475794, 4476156, 4476711, 4477004, 4477133, 4477189, 4477676,
4477831, 4477900, 4477973, 4477994, 4478011, 4478040, 4478063, 4478085, 4478468, 4478715, 4479515, 4480034, 4481804, 4483259,
4483866, 4484202, 4484932, 4485693, 4486184, 4486549, 4486869, 4487405, 4487639, 4487845, 4488086, 4488256, 4488505, 4488714,
4492669, 4496233, 4497738, 4498122, 4498653, 4499862, 4501169, 4501627, 4501673, 4501811, 4502182, 4502475, 4502533, 4502542,
4502548, 4502733, 4503389, 4504381, 4505070, 4505378, 4505814, 4506031, 4506336, 4506642, 4506845, 4506971, 4506986, 4507016,
4507051, 4507098, 4507107, 4507114, 4507139, 4507478, 4507643, 4507674, 4507694, 4507814, 4507894, 4507904, 4507929, 4507989,
4508023, 4508047, 4508053, 4508063, 4508075, 4508092, 4508104, 4508113, 4508239, 4508285, 4508324, 4508335, 4508340, 4508378,
4508405, 4508419, 4508436, 4508449, 4508470, 4508488, 4508515, 4508541, 4508564, 4508570, 4508584, 4508594, 4508607, 4508634,
4508652, 4508665, 4508673, 4508692, 4508704, 4508742, 4508755, 4508773, 4508788, 4508798, 4508832, 4508869, 4508885, 4508905,
4508915, 4508947, 4508956, 4509061, 4509070, 4509357, 4509368, 4509380, 4509393, 4509401, 4509412, 4509426, 4509438, 4509451,
4509461, 4509473, 4509489, 4509498, 4509512, 4509537, 4509568, 4509582, 4509621, 4509629, 4509747, 4509766, 4509776, 4509795,
4509802, 4509813, 4509822, 4509829, 4509834, 4509844, 4509854, 4509863, 4509868, 4509875, 4509886, 4509898, 4509908, 4509920,
4509932, 4509941, 4509949, 4509955, 4509967, 4509972, 4509979, 4509987, 4509999, 4510002, 4510010, 4510014, 4510018, 4510025,
4510028, 4510049, 4510055, 4510061, 4510068, 4510079, 4510085, 4510091, 4510098, 4510102, 4510104, 4510110, 4510121, 4510128,
4510132, 4510138, 4510144, 4510145, 4510153, 4510161, 4510174, 4510196, 4510199, 4510208, 4510209, 4510212, 4510216, 4510217,
4510219, 4510222, 4510228, 4510231, 4510236, 4510241, 4510245, 4510248, 4510250, 4510254, 4510255, 4510261, 4510262, 4510266,
4510266, 4510271, 4510285, 4510287, 4510291, 4510295, 4510303, 4510306, 4510308, 4510310, 4510314, 4510319, 4510320, 4510324,
4510328, 4510333, 4510333, 4510336, 4510340, 4510342, 4510348, 4510353, 4510359, 4510362, 4510365, 4510371, 4510373, 4510375,
4510378, 4510380, 4510385, 4510389, 4510391, 4510391, 4510394, 4510396, 4510397, 4510398, 4510400, 4510403, 4510406, 4510407,
4510408, 4510409, 4510411, 4510413, 4510417, 4510417, 4510419, 4510422, 4510426, 4510427, 4510430, 4510435, 4510437, 4510439,
4510440, 4510442, 4510442, 4510446, 4510447, 4510448, 4510450, 4510451, 4510451, 4510453, 4510454, 4510455, 4510457, 4510460,
4510460, 4510460, 4510462, 4510463, 4510466, 4510468, 4510472, 4510475, 4510480, 4510482, 4510483, 4510486, 4510488, 4510492,
4510494, 4510497, 4510497, 4510499, 4510503, 4510505, 4510506, 4510507, 4510509, 4510512, 4510514, 4510527, 4510551, 4510553,
4510554, 4510555, 4510556, 4510558, 4510561, 4510562, 4510566, 4510567, 4510568, 4510570, 4510573, 4510574, 4510586, 4510603,
4510605, 4510607, 4510610, 4510610, 4510613, 4510613, 4510614, 4510614, 4510615, 4510616, 4510616, 4510620, 4510622, 4510623,
4510624, 4510627, 4510628, 4510630, 4510631, 4510632, 4510634, 4510634, 4510634, 4510636, 4510636, 4510639, 4510639, 4510640,
4510643, 4510647, 4510649, 4510650, 4510653, 4510653, 4510653, 4510653, 4510656, 4510659, 4510661, 4510664, 4510665, 4510669,
4510672, 4510673, 4510674, 4510675, 4510680, 4510683, 4510684, 4510686, 4510687, 4510690, 4510691, 4510693, 4510693, 4510697,
4510699, 4510700, 4510703, 4510704, 4510709, 4510711, 4510713, 4510713, 4510720, 4510720, 4510722, 4510724, 4510727, 4510729,
4510735, 4510735, 4510738, 4510740, 4510744, 4510745, 4510746, 4510748, 4510754, 4510756, 4510758, 4510761, 4510764, 4510766,
4510768, 4510768, 4510770, 4510770, 4510772, 4510774, 4510775, 4510775, 4510775, 4510776, 4510777, 4510780, 4510782, 4510783,
4510785, 4510786, 4510788, 4510789, 4510791, 4510793, 4510793, 4510793, 4510795, 4510795, 4510799, 4510803, 4510804, 4510804,
4510804, 4510805, 4510807, 4510809, 4510811, 4510811, 4510813, 4510815, 4510815, 4510816, 4510819, 4510820, 4510824, 4510827,
4510829, 4510829, 4510830, 4510833, 4510835, 4510837, 4510838, 4510838, 4510839, 4510840, 4510840, 4510842, 4510842, 4510843,
4510845, 4510845, 4510845, 4510847, 4510848, 4510848, 4510848, 4510850, 4510853, 4510855, 4510857, 4510859, 4510861, 4510862,
4510864, 4510865, 4510865, 4510865, 4510869, 4510869, 4510869, 4510869, 4510869, 4510870, 4510870, 4510872, 4510872, 4510873,
4510874, 4510875, 4510875, 4510877, 4510879, 4510879, 4510879, 4510879, 4510880, 4510881, 4510882, 4510883, 4510884, 4510885,
4510886, 4510887, 4510890, 4510890, 4510891, 4510892, 4510892, 4510893, 4510893, 4510895, 4510895, 4510896, 4510897, 4510899,
4510901, 4510901, 4510901, 4510902, 4510903, 4510903, 4510903, 4510905, 4510905, 4510906, 4510906, 4510907, 4510907, 4510909,
4510910, 4510911, 4510911, 4510911, 4510913, 4510913, 4510914, 4510914, 4510914, 4510915, 4510916, 4510918, 4510918, 4510919,
4510919, 4510919, 4510920, 4510921, 4510922, 4510923, 4510924, 4510924, 4510924, 4510924, 4510926, 4510927, 4510928, 4510928,
4510928, 4510928, 4510928, 4510930, 4510933, 4510935, 4510935, 4510935, 4510935, 4510935, 4510936, 4510938, 4510947, 4510966,
4510967, 4510969, 4510973, 4510973, 4510974, 4510974, 4510974, 4510974, 4510974, 4510974, 4510975, 4510976, 4510976, 4510976,
4510976, 4510976, 4510976, 4510976, 4510977, 4510979, 4510979, 4510979, 4510979, 4510979, 4510979, 4510980, 4510980, 4510980,
4510980, 4510981, 4510981, 4510981, 4510982, 4510982, 4510982, 4510982, 4510982, 4510982, 4510982, 4510983, 4510983, 4510984,
4510984, 4510984, 4510984, 4510984, 4510985, 4510985, 4510985, 4510985, 4510987, 4510987, 4510987, 4510988, 4510988, 4510989,
4510989, 4510989, 4510989, 4510989, 4510990, 4510990, 4510990, 4510990, 4510990, 4510990, 4510990, 4510991, 4510991, 4510991,
4510991, 4510991, 4510991, 4510991, 4510992, 4510992, 4510992, 4510992, 4510992, 4510992, 4510992, 4510993, 4510993, 4510993,
4510994, 4510994, 4510994, 4510994, 4510995, 4510995, 4510996, 4510997, 4510998, 4510999, 4510999, 4511000, 4511000, 4511001,
4511001, 4511002, 4511002, 4511002, 4511003, 4511004, 4511004, 4511004, 4511004, 4511005, 4511006, 4511008, 4511008, 4511008,
4511009, 4511009, 4511009, 4511009, 4511010, 4511011, 4511011, 4511012, 4511012, 4511012, 4511012, 4511013, 4511013, 4511014,
4511014, 4511014, 4511014, 4511015, 4511018, 4511018, 4511018, 4511018, 4511018, 4511018, 4511018, 4511020, 4511020, 4511020,
4511020, 4511020, 4511020, 4511020, 4511021, 4511021, 4511021, 4511021, 4511021, 4511021, 4511021, 4511021, 4511021, 4511021,
4511021
  };

  rnd %= utable[1024];
  l = 0; r = 1023;
  while (l < r)
    {
      m = (l+r)/2;
      if (utable[m] == rnd)
	return m;
      if (utable[m] >= rnd)
	r = m - 1;
      else
	l = m + 1;
    }
  return l;
}

static uns
gen_size(uns min, uns max, uns rnd)
{
  if (min == max)
    return min;
  else
    return min + rnd % (max - min + 1);
}

static void
gen_random(byte *buf, uns size, uns kn)
{
  kn = (kn + 0x36221057) ^ (kn << 24) ^ (kn << 15);
  while (size--)
    {
      *buf++ = kn >> 24;
      kn = kn*257 + 17;
    }
}

static int
keygen(byte *buf, uns kn)
{
  uns size, rnd;

  rnd = krand(kn);
  if (key_min < 0)
    size = gen_url_size(rnd);
  else
    size = gen_size(key_min, key_max, rnd);
  *buf++ = kn >> 24;
  *buf++ = kn >> 16;
  *buf++ = kn >> 8;
  *buf++ = kn;
  if (size < 4)
    return 4;
  gen_random(buf, size-4, kn);
  return size;
}

static int
valgen(byte *buf, uns kn)
{
  uns size = gen_size(val_min, val_max, krand(kn));
  gen_random(buf, size, kn);
  return size;
}

static uns
keydec(byte *buf)
{
  return (buf[0] << 24) | (buf[1] << 16) | (buf[2] << 8) | buf[3];
}

static void
verb(char *msg, ...)
{
  int cat = 1;
  va_list args;

  va_start(args, msg);
  if (msg[0] == '^' && msg[1])
    {
      cat = msg[1] - '0';
      msg += 2;
    }
  if (verbose >= cat)
    vfprintf(stderr, msg, args);
  va_end(args);
}

static void
parse_size(int *min, int *max, char *c)
{
  char *d;

  if ((d = strchr(c, '-')))
    {
      *d++ = 0;
      *min = atol(c);
      *max = atol(d);
    }
  else
    *min = *max = atol(c);
}

#define PROGRESS(i) if ((verbose > 2) || (verbose > 1 && !(i & 1023))) fprintf(stderr, "%d\r", i)

int main(int argc, char **argv)
{
  int c, i, j, k, l, m;
  byte kb[2048], vb[2048], vb2[2048];
  uns ks, vs, vs2, perc, cnt;
  char *ch;
  int dont_delete = 0;
  timestamp_t timer;

  log_init("dbtest");
  setvbuf(stdout, NULL, _IONBF, 0);
  setvbuf(stderr, NULL, _IONBF, 0);
  while ((c = getopt(argc, argv, "c:p:k:n:d:vsStF")) >= 0)
    switch (c)
      {
      case 'c':
	opts.cache_size = atol(optarg);
	break;
      case 'p':
	opts.page_order = atol(optarg);
	break;
      case 'k':
	if (!strcmp(optarg, "U"))
	  key_min = key_max = -1;
	else
	  parse_size(&key_min, &key_max, optarg);
	break;
      case 'n':
	num_keys = atol(optarg);
	break;
      case 'd':
	parse_size(&val_min, &val_max, optarg);
	break;
      case 'v':
	verbose++;
	break;
      case 's':
	opts.flags |= SDBM_SYNC;
	break;
      case 'S':
	opts.flags |= SDBM_SYNC | SDBM_FSYNC;
	break;
      case 'F':
	opts.flags |= SDBM_FAST;
	break;
      case 't':
	dont_delete = 1;
	break;
      default:
	help();
      }

  if (key_min >= 0 && key_min < 4)
    key_min = key_max = 4;
  if (key_min == key_max && key_min >= 0)
    opts.key_size = key_min;
  if (val_min == val_max)
    opts.val_size = val_min;
  if (!num_keys)
    die("Number of keys not given");

  printf(NAME " benchmark: %d records, keys ", num_keys);
  if (key_min < 0)
    printf("<URL>");
  else
    printf("%d-%d", key_min, key_max);
  printf(", values %d-%d, page size %d, cache %d pages\n", val_min, val_max, 1 << opts.page_order, opts.cache_size);

  verb("OPEN(%s, key=%d, val=%d, cache=%d, pgorder=%d)\n", opts.name, opts.key_size, opts.val_size,
       opts.cache_size, opts.page_order);
  if (!dont_delete)
    unlink(opts.name);
  d = sdbm_open(&opts);
  if (!d)
    die("open failed: %m");

  while (optind < argc)
    {
      char *o = argv[optind++];
      init_timer(&timer);
      switch (*o)
	{
	case 'c':
	  printf("create %d: ", num_keys);
	  for(i=0; i<num_keys; i++)
	    {
	      PROGRESS(i);
	      ks = keygen(kb, i);
	      vs = valgen(vb, i);
	      if (sdbm_store(d, kb, ks, vb, vs) != 1) die("store failed");
	    }
	  break;
	case 'r':
	  printf("rewrite %d: ", num_keys);
	  for(i=0; i<num_keys; i++)
	    {
	      PROGRESS(i);
	      ks = keygen(kb, i);
	      vs = valgen(vb, i);
	      if (sdbm_replace(d, kb, ks, vb, vs) != 1) die("replace failed");
	    }
	  break;
	case 'f':
	case 'F':
	  c = (*o++ == 'f');
	  if ((ch = strchr(o, '%')))
	    {
	      *ch++ = 0;
	      perc = atol(o);
	    }
	  else
	    {
	      ch = o;
	      perc = 100;
	    }
	  cnt = atol(ch);
	  if (!cnt)
	    {
	      cnt = num_keys;
	      m = (perc == 100);
	    }
	  else
	    m = 0;
	  printf("%s fetch %d (%d%% success, with%s values): ", (m ? "sequential" : "random"), cnt, perc, (c ? "" : "out"));
	  i = -1;
	  while (cnt--)
	    {
	      if (m)
		i++;
	      else
		i = random_max(num_keys) + ((random_max(100) < perc) ? 0 : num_keys);
	      PROGRESS(i);
	      ks = keygen(kb, i);
	      if (c)
		{
		  vs2 = sizeof(vb2);
		  j = sdbm_fetch(d, kb, ks, vb2, &vs2);
		}
	      else
		j = sdbm_fetch(d, kb, ks, NULL, NULL);
	      if (j < 0)
		die("fetch: error %d", j);
	      if ((i < num_keys) != j)
		die("fetch mismatch at key %d, res %d", i, j);
	      if (c && j)
		{
		  vs = valgen(vb, i);
		  if (vs != vs2 || memcmp(vb, vb2, vs))
		    die("fetch data mismatch at key %d: %d,%d", i, vs, vs2);
		}
	    }
	  break;
	case 'd':
	  printf("delete %d: ", num_keys);
	  for(i=0; i<num_keys; i++)
	    {
	      PROGRESS(i);
	      ks = keygen(kb, i);
	      if (sdbm_delete(d, kb, ks) != 1) die("delete failed");
	    }
	  break;
	case 'w':
	case 'W':
	  c = (*o == 'w');
	  i = k = l = m = 0;
	  printf("walk %d (with%s keys): ", num_keys, (c ? "" : "out"));
	  sdbm_rewind(d);
	  for(;;)
	    {
	      ks = sizeof(kb);
	      vs = sizeof(vb);
	      if (c)
		j = sdbm_get_next(d, kb, &ks, vb, &vs);
	      else
		j = sdbm_get_next(d, kb, &ks, NULL, NULL);
	      if (!j)
		break;
	      if (ks < 4)
		die("get_next: too short");
	      i = keydec(kb);
	      if (i < 0 || i >= num_keys)
		die("get_next: %d out of range", i);
	      PROGRESS(i);
	      vs2 = keygen(vb2, i);
	      if (ks != vs2 || memcmp(kb, vb2, ks))
		die("get_next: key mismatch at %d", i);
	      if (c)
		{
		  vs2 = valgen(vb2, i);
		  if (vs != vs2 || memcmp(vb, vb2, vs))
		    die("get_next: data mismatch at %d", i);
		}
	      l += k;
	      m += i;
	      k++;
	    }
	  if (k != num_keys)
	    die("fetch: wrong # of keys: %d != %d", k, num_keys);
	  if (l != m)
	    die("fetch: wrong checksum: %d != %d", l, m);
	  break;
	default:
	  help();
	}
      sdbm_sync(d);
      printf("%d ms\n", get_timer(&timer));
    }

  verb("CLOSE\n");
  sdbm_close(d);

  {
    struct stat st;
    if (stat(opts.name, &st)) die("stat: %m");
    printf("file size: %d bytes\n", (int) st.st_size);
  }
  return 0;
}
