
#include <inttypes.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <utime.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <pthread.h>

#include <getopt.h>

/***************************************************************************/

/* структура - односвязный список, описывающая файлы */
typedef struct dirlist {
   char* name; /* имя файла */
   time_t date; /* тайм штамп */
   int done; /* флаг, указывающий, был ли этот файл уже скопирован */
   u_int64_t size; /* размер файла */
   struct dirlist* next; /* указатель на следующий элемент списка */
} dirlist;

typedef struct dirinfo_t {
   u_int64_t nfiles;
   u_int64_t size;
} dirinfo;

/* структура данных потока */
typedef struct thread_data {
   dirlist* dlist; /* указатель на список файлов */
   const char* srcdir; /* имя исходного каталога */
   const char* dstdir; /* имя каталога назначения */
} thread_data;

int threads_count = 0; /* счетчик кол-ва потоков */
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER; /* мьютекс блокировки доступа потоков к списку файлов */

/***************************************************************************/

const char* readable_fs(char *buf, u_int64_t fsize);

const char* readable_pthread_t(char *buf, pthread_t pt);

/* функция, читает содержимое каталога в список */
dirlist* read_dir_tree(dirlist* dlist, const char* path);

/* выделяет память для элемента списка */
dirlist* alloc_next();

/* связывает элементы списка */
dirlist* link_nodes(dirlist* left, const dirlist* right);

/* получает следующий, еще не скопированный файл */
dirlist* get_next(dirlist* dlist);

/* возвращает кол-во файлов в списке */
void get_dirinfo(dirinfo *di, const dirlist* dlist);

/* создает имя результирующего файла */
char* create_dst_filename(const char* srcdir, const char* srcname, const char* dstdir);

/* создает имя исходного файла */
char* create_src_filename(const char* dstdir, const char* dstname, const char* srcdir);

/* извлекает имя каталога */
char* extract_path(const char* filename);

/* создает структуру каталога */
int create_dir_tree(const char* dirname);

/* копирует файл */
int copy_file(const char* srcname, const char* dstname, time_t srctime);

/* возвращает список файлов которые необходимо скопировать */
dirlist* get_difference(
   dirlist* result,
   const dirlist* srclist, const char* srcdir,
   const dirlist* dstlist, const char* dstdir
);

/* функция потока выполняющая копирование файлов */
void* thread_proc(void* p);

void usage(const char* pname) {
   char* p = strrchr(pname, '/');
   p = (p)?p+1:"dsync2";

   static const char* usage_string =
   "\t--src=dir_name     --  source directory name\n"
   "\t--dst=dir_name     --  destination directory name\n"
   "\t--symlinks=yes|no  --  read symlinks\n"
   "\t--threads=N        --  number of worker threads\n"
   "\t--info             --  show statistic at finish\n"
   "\t--version          --  show program version\n"
   ;
   fprintf(stdout,
      "usage: %s [OPTIONS]\n%s",
      p,
      usage_string
   );
   fflush(stdout);
}

#define DSYNC2_VERSION "0.0.1"

void version(const char *pname) {
   char* p = strrchr(pname, '/');
   p = (p)?p+1:"dsync2";
   printf("%s version %s\n", p, DSYNC2_VERSION);
}

/***************************************************************************/

int main(int argc, char** argv) {
   /**  */
   if ( argc < 3 ) {
      usage(argv[0]);
      return 1;
   }

   char sizebuf[32] = {0};

   /** flags */
   int show_info = 0;
   int show_version = 0;

   /**  */
   const char* srcdir = NULL; /* имя исходного каталога */
   const char* dstdir = NULL; /* имя каталога назначения */

   /**  */
   unsigned idx = 0;
   unsigned nthreads = 2; /* кол-во потоков копирования */

   /**  */
   dirlist srclist = {0,0,0,0,0}; /* список файлов в исходном каталоге */
   dirlist dstlist = {0,0,0,0,0}; /* список файлов в каталоге назначения */
   dirlist result  = {0,0,0,0,0}; /* список файлов к копированию */

   /**  */
   dirinfo srcdi = {0,0};
   dirinfo dstdi = {0,0};
   dirinfo tocopy= {0,0};

   /**  */
   pthread_t* threads; /* указатель на потоки копирования */

   /* заполняю структуру данных потоков */
   thread_data thdata;

   /**  */
   static struct option long_options[] = {
      {"src", required_argument, 0, 's'},
      {"dst", required_argument, 0, 'd'},
      {"threads", required_argument, 0, 't'},
      {"info", no_argument, 0, 'i'},
      {"version", no_argument, 0, 'v'},
      {0,0,0,0}
   };

   while ( 1 ) {
      int opt = 0, option_index = 0;
      opt = getopt_long(
               argc, argv,
               "s:d:t:iv",
               long_options,
               &option_index
            );
      if ( opt == -1 ) break;

      switch ( opt ) {
         case 's': srcdir = optarg; break;
         case 'd': dstdir = optarg; break;
         case 't': nthreads=atoi(optarg); break;
         case 'i': show_info=1; break;
         case 'v': show_version=1; break;
         default: usage(argv[0]); exit(1);
      }
   }

   if ( show_version ) {
   	  version(argv[0]);
      return 0;
   }

   if ( !srcdir ) {
      printf("source directory is not specified! terminate.\n");
      return 1;
   }
   if ( !dstdir ) {
      printf("destination directory is not specified! terminate.\n");
      return 1;
   }

   /**  */
   if ( access(srcdir, F_OK) ) {
      printf("source directory is not exists! terminate.\n");
      return 1;
   }
   if ( access(dstdir, F_OK) ) {
      printf("destination directory is not exists! terminate.\n");
      return 1;
   }

   /* читаю содержимое исходного каталога */
   read_dir_tree(&srclist, srcdir);
   read_dir_tree(&dstlist, dstdir);

   // /* получаю кол-во файлов и объем */
   get_dirinfo(&srcdi, &srclist);
   get_dirinfo(&dstdi, &dstlist);

   /* если исходный каталог пуст, сообщаю, завершаюсь */
   if ( 0 == srcdi.nfiles ) {
      printf("source dir is empty! terminate.\n");
      return 0;
   }

   /* если указанно не верно - сообщаю, завершаюсь */
   if ( nthreads <= 0 || nthreads > srcdi.nfiles ) {
      printf("wrong num of threads. terminate.\n");
      return 0;
   }

   if ( show_info ) {
      /* вывожу информацию о исходном каталоге */
      printf("source dir contains      %5" PRIu64 " files with total size %s\n",
         srcdi.nfiles,
         readable_fs(sizebuf, srcdi.size)
      );

      /* вывожу информацию о каталоге назначения */
      printf("destination dir contains %5" PRIu64 " files with total size %s\n",
         dstdi.nfiles,
         readable_fs(sizebuf, dstdi.size)
      );
   }

   /* получаю список файлов которые необходимо скопировать */
   get_difference(&result, &srclist, srcdir, &dstlist, dstdir);

   /* получаю кол-во файлов и суммарный объем */
   get_dirinfo(&tocopy, &result);

   /* если кол-во файлов равно нулю, значит каталоги
      идентичны. сообщаю. завершаюсь.
   */
   if ( 0 == tocopy.nfiles ) {
      printf("\nthe directories are identical. terminate.\n");
      return 0;
   }

   if ( show_info ) {
      /* вывожу информацию */
      printf("need to copy %" PRIu64 " files with total size %s\n",
         tocopy.nfiles,
         readable_fs(sizebuf, tocopy.size)
      );
   }

   thdata.dlist = &result;
   thdata.srcdir= srcdir;
   thdata.dstdir= dstdir;

   /* сохраняю кол-во потоков для последующего использования
      в цикле ожидания завершения копирования
   */
   threads_count = nthreads;

   /* выделяю память для указателей потока */
   threads = (pthread_t*)malloc(nthreads*sizeof(pthread_t));

   /* создаю необходимое кол-во потоков */
   for ( idx = 0; idx < nthreads; idx++ ) {
      pthread_create(&threads[idx], NULL, thread_proc, &thdata);
   }

   /* повторяю цикл, до тех пор, пока кол-во рабочих потоков не равно нулю */
   while ( threads_count ) {
      usleep(1000);
   }

   return 0;
}

/***************************************************************************/
/* функция потока которая производит копирование файлов */
void* thread_proc(void* p) {
   int err;
   char printbuf[32] = {0};
   /* получаю идентификатор потока */
   pthread_t pid = pthread_self();
   /* сообщаю */
   printf("process ID %s created\n", readable_pthread_t(printbuf, pid));
   /* нормализую указатель на данные потока */
   thread_data* data = (thread_data*)p;
   /* получаю список файлов необходимых к копированию */
   dirlist* list = data->dlist;
   /* указатель на один элемент. используется далее */
   dirlist* node = NULL;
   /* бесконечный цикл */
   while ( 1 ) {
      /* блокирую остальные потоки */
      pthread_mutex_lock(&mutex);
      /* получаю следующий элемент */
      node = get_next(list);
      /* если равен NULL, значит все файлы скопированы */
      if ( !node ) {
         /* снимаю блокировку */
         pthread_mutex_unlock(&mutex);
         /* завершаю поток */
         break;
      }
      /* устанавливаю флаг выполненого элемента */
      node->done = 1;
      /* создаю полное имя для создания файла назначения */
      char* name = create_dst_filename(data->srcdir, node->name, data->dstdir);
      /* извлекаю путь */
      char* path = extract_path(name);
      /* создаю структуру каталогов */
      create_dir_tree(path);
      /* снимаю блокировку */
      pthread_mutex_unlock(&mutex);
      /* сообщаю о копировании */
      printf("process ID %s copying: %s\n", readable_pthread_t(printbuf, pid), node->name);
      /* копирую */
      if ( 0 != (err=copy_file(node->name, name, node->date)) ) {
         fprintf(stderr, "error: %s\n", strerror(err));
      }
   }
   /* декрементирую счетчик запущеных потоков перед выходом */
   threads_count--;
   /* выхожу */
   return NULL;
}

/***************************************************************************/
/* читает содержимое каталога */
dirlist* read_dir_tree(dirlist* dlist, const char* path) {
   char curname[1024] = "\0";
   struct stat st;
   struct dirent* dirent;
   /* открываю каталог для чтения его содержимого */
   DIR* dir = opendir(path);
   /* если не открылся, завершаюсь */
   if ( !dir ) {
      fprintf(stderr, "error opening directory: %s\n", strerror(errno));
      return dlist;
   }
   /* повторяется пока есть элементы в каталоге */
   while ( (dirent = readdir(dir)) != NULL ) {
      /* если имя каталога "." или ".." читаю следующий */
      if ( !strcmp(dirent->d_name, ".") || !strcmp(dirent->d_name, "..") )
         continue;
      if ( strlen(path)+strlen("/")+strlen(dirent->d_name) >= sizeof(curname) ) {
         fprintf(stderr, "file name is too long: \"%s\"\n", curname);
         exit(1);
      }
      strcpy(curname, path);
      strcat(curname, "/");
      strcat(curname, dirent->d_name);
      /* если прочитано имя каталога, перехожу в него */
      if ( dirent->d_type & DT_DIR ) {
         dlist = read_dir_tree(dlist, curname);
      /* если прочитано имя файла, получаю информацию о нем */
      } else if ( dirent->d_type & DT_REG ) {
         if ( -1 == stat(curname, &st) ) {
            fprintf(stderr, "error: stat(%s)\n", curname);
            exit(1);
         }
         /* заношу информацию о файле в нод */
         dlist->name = strdup(curname);
         dlist->date = st.st_mtime;
         dlist->size = st.st_size;
         dlist->next = alloc_next();
         dlist = dlist->next;
      }
   }
   /* закрываю каталог */
   closedir(dir);
   return dlist;
}
/* выделяет память для нода */
dirlist* alloc_next() {
   dirlist* next = (dirlist*)malloc(sizeof(dirlist));
   memset(next, 0, sizeof(dirlist));
   return next;
}
/* создает полное имя файла назначения из компонент */
char* create_dst_filename(const char* srcdir, const char* srcname, const char* dstdir) {
   const char* name = srcname+strlen(srcdir);
   char* result = (char*)malloc(strlen(name)+strlen(dstdir)+1);
   strcpy(result, dstdir);
   strcat(result, name);
   return result;
}
/* создает полное имя исходного файла из компонент */
char* create_src_filename(const char* dstdir, const char* dstname, const char* srcdir) {
   const char* name = dstname+strlen(dstdir);
   char* result = (char*)malloc(strlen(name)+strlen(srcdir)+1);
   strcpy(result, srcdir);
   strcat(result, name);
   return result;
}
/* находит нод по имени файла */
const dirlist* find_by_filename(const dirlist* dlist, const char* fname) {
   while ( dlist->name ) {
      if ( 0 == strcmp(dlist->name, fname) ) return dlist;
      dlist = dlist->next;
   }
   return NULL;
}
/* возвращает разницу в виде списка файлов готовых к копированию */
dirlist* get_difference(
   dirlist* result,
   const dirlist* srclist, const char* srcdir,
   const dirlist* dstlist, const char* dstdir
) {
   const dirlist* src_ptr = srclist;
   const dirlist* dst_ptr = dstlist;
   dirlist* res_ptr = result;
   dirinfo dst_files = {0,0};
   dirinfo src_files = {0,0};
   get_dirinfo(&dst_files, dstlist);
   get_dirinfo(&src_files, srclist);
   /* если каталог назначения пуст, просто копирую весь список файлов */
   if ( 0 == dst_files.nfiles ) {
      while ( src_ptr->name ) {
         res_ptr = link_nodes(res_ptr, src_ptr);
         src_ptr = src_ptr->next;
      }
   /* если кол-во файлов в обоих каталогах равно, сверяю их дату */
   } else if ( src_files.nfiles == dst_files.nfiles ) {
      while ( src_ptr->name ) {
         char* test_name = create_dst_filename(srcdir, src_ptr->name, dstdir);
         /* если в каталоге назначения файл есть, и его дата позднее
            исходного файла, пропускаю этот файл */
         if ( 0 == access(test_name, F_OK) && src_ptr->date <= dst_ptr->date ) {
            free(test_name);
            src_ptr = src_ptr->next;
            dst_ptr = dst_ptr->next;
            continue;
         }
         free(test_name);
         /* добавляю к списку копируемых файлов */
         res_ptr = link_nodes(res_ptr, src_ptr);
         src_ptr = src_ptr->next;
         dst_ptr = dst_ptr->next;
      }
   /* если исходный каталог не пуст, и кол-во файлов в обоих каталогах не
      равно, проверяю все файлы на несоответствие даты */
   } else {
      /* сверяю все файлы из каталога назначения с файлами в исходном каталоге */
      while ( dst_ptr->name ) {
         /* создаю полное имя исходного файла */
         char* test_name = create_src_filename(dstdir, dst_ptr->name, srcdir);
         /* ищу в исходном каталоге файл с таким именем */
         const dirlist* node = find_by_filename(srclist, test_name);
         /* освобождаю память */
         free(test_name);
         /* если в исходном каталоге нет такого файла, продолжаю поиск следующего */
         if ( !node ) {
            dst_ptr = dst_ptr->next;
            continue;
         }
         /* если есть, сверяю дату */
         if ( dst_ptr->date < node->date ) {
            /* если в исходном каталоге файл новее, добавляю его к списку копируемых */
            res_ptr = link_nodes(res_ptr, node);
         }
         /* беру следующий файл */
         dst_ptr = dst_ptr->next;
      }
      src_ptr = srclist;
      /* сверяю все файлы из исходного каталога с файлами в каталоге назначения */
      while ( src_ptr->name ) {
         /* создаю полное имя файла назначения */
         char* test_name = create_dst_filename(srcdir, src_ptr->name, dstdir);
         /* ищу в каталоге назначения файл с этим именем */
         const dirlist* node = find_by_filename(dstlist, test_name);
         /* освобождаю память */
         free(test_name);
         /* если есть, ищу следующий */
         if ( node ) {
            src_ptr = src_ptr->next;
            continue;
         }
         /* добавляю в список копируемых */
         res_ptr = link_nodes(res_ptr, src_ptr);
         /* беру следующий файл */
         src_ptr = src_ptr->next;
      }
   }
   return result;
}
/* возвращает следующий готовый к копированию элемент */
dirlist* get_next(dirlist* dlist) {
   while ( dlist->name ) {
      if ( !dlist->done ) {
         return dlist;
      }
      dlist = dlist->next;
   }
   return NULL;
}
/* извлекает имя каталога из полного имени файла */
char* extract_path(const char* filename) {
   const char* p = strrchr(filename, '/');
   if ( !p ) return NULL;
   char* buff = (char*)malloc(p-filename+1);
   strncpy(buff, filename, p-filename);
   buff[p-filename] = 0;
   return buff;
}
/* создает структуру каталогов */
int create_dir_tree(const char* dirname) {
   char temp[1024] = "\0";
   char* pname = &temp[0];
   char* end = (char*)strchr(dirname+1, '/');
   while ( 1 ) {
      strncpy(pname, dirname, end-dirname);
      if ( 0 == access(pname, F_OK) ) {
         if ( 0 == strcmp(pname, dirname) ) break;
         end = strchr(end+1, '/');
         if ( !end ) {
            end = (char*)dirname+strlen(dirname);
         }
         continue;
      }
      if ( 0 != mkdir(pname, S_IRWXU|S_IRWXG|S_IRWXO) ) {
         fprintf(stderr, "error: %s\n", strerror(errno));
      }
      if ( 0 == strcmp(pname, dirname) ) break;
      end = strchr(end+1, '/');
      if ( !end ) {
         end = (char*)dirname+strlen(dirname);
      }
   }

   return 0;
}
/* копирует файл */
int copy_file(const char* srcname, const char* dstname, time_t srctime) {
   struct utimbuf time;
   time.modtime = srctime;
   time.actime  = 0;
   /**  */
   FILE* in = fopen(srcname, "rb");
   if ( !in ) { return errno; }
   FILE* out= fopen(dstname, "wb");
   if ( !out) {
      fclose(in);
      return errno;
   }
   const int bsize = 1024*256;
   char buff[bsize];

   int rd;
   while ( (rd=fread(buff, 1, bsize, in)) > 0 ) {
      fwrite(buff, 1, rd, out);
   }
   fclose(in);
   fclose(out);
   /* устанавливаю дату модификации скопированого файла
      равную дате исходного */
   utime(dstname, &time);

   return 0;
}
/* связывает ноды */
dirlist* link_nodes(dirlist* left, const dirlist* right) {
   left->name = right->name;
   left->date = right->date;
   left->size = right->size;
   left->next = alloc_next();
   return left->next;
}

void get_dirinfo(dirinfo *di, const dirlist* dlist) {
   u_int64_t count = 0;
   u_int64_t size = 0;
   while ( dlist->name ) {
      count++;
      size += dlist->size;

      dlist = dlist->next;
   }

   di->nfiles = count;
   di->size = size;
}

const char* readable_fs(char *buf, u_int64_t fsize) {
   double size = (double)fsize;
   unsigned i = 0;
   static const char* units[] = {"B", "kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"};
   while (size > 1024u) {
      size /= 1024u;
      i++;
   }

   sprintf(buf, "%.*f %s", i, size, units[i]);

   return buf;
}

const char* readable_pthread_t(char *buf, pthread_t pt) {
   char *p = buf;
   unsigned char *ptc = (unsigned char*)(void*)(&pt);
   int n = sprintf(p, "0x");
   p += n;
   for ( size_t i = 0; i < sizeof(pt); i++ ) {
      n = sprintf(p, "%02x", (unsigned)(ptc[i]));
      p += n;
   }
   *p = 0;
   return buf;
}
