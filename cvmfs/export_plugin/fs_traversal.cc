

fs_traverse_dir(const char *dir, struct fs_traversal *src,  struct fs_traversal *dest){
  char **src_dir = NULL;
  size_t src_len = 0;
  size_t src_iter = 0;
  struct cvmfs_stat src_st;
  src->listdir(dir, &src_dir, &src_len);
  if(src_dir[src_iter]){
    src->ext_stat(src_dir[src_iter], &src_st);
  }

  char **dest_dir = NULL;
  size_t dest_len = 0;
  size_t dest_iter = 0;
  struct cvmfs_stat dest_st;
  dest->listdir(dir, &dest_dir, &dest_len);
  if(dest_dir[dest_iter]){
    dest->ext_stat(dest_dir[dest_iter], &dest_st);
  }

  /* While both have defined members to compare. */
  while(src_dir[src_iter] && dest_iter[dest_iter]){
    /* Check if item is present in both */
    int cmp = strcmp(src_dir[src_iter], dest_iter[dest_iter]);
    if(cmp == 0){
      /* Compares stats to see if they are equivalent */
      if(!cmvfs_stat_cmp(src_st, dest_st)){
        /* If not equal, bring dest up-to-date */
        switch (src_st->st_mode & S_IFMT) {
          case S_IFREG:
            /* They don't point to the same data, link new data */
            if(!dest->has_hash(src_st.hash)){
              dest->copy(src_st.hash, src_st.hash);
            }
            dest->link(dest_dir[dest_iter], src_st.hash);
            // TODO Apply attributes
            break;
          case S_IFDIR:
            // TODO Apply attributes
            break;
          case S_IFLNK:
            break;
        }
      /* No else stmt, as they are equivalent */
      }
      src_iter++;
      dest_iter++;

    /* Src contains something missing from Dest */
    } else if (cmp < 0) {
       switch (src_st->st_mode & S_IFMT) {
        case S_IFREG:
          /* They don't point to the same data, link new data */
          if(!dest->has_hash(src_st.hash)){
            dest->copy(src_st.hash, src_st.hash);
          }
          dest->link(dest_dir[dest_iter], src_st.hash);
          // TODO Apply attributes
          break;
        case S_IFDIR:
          dest->mkdir(src_dir[src_iter]);
          fs_traverse_dir(src_dir[src_iter], src, dest); 
          break;
        case S_IFLNK:
          break;
      }
      src_iter++;
    /* Dest contains something missing from Src */
    } else if (cmp > 0) {
       switch (dest_st->st_mode & S_IFMT) {
        case S_IFREG:
        case S_IFLNK:
          dest->unlink(dest_dir[dest_iter]);
          break;
        case S_IFDIR:
          fs_traverse_dir(dest_dir[dest_iter], src, dest); 
          dest->rmdir(dest_dir[dest_iter]);
          break;
      }
      dest_iter++;
    }
  }
}
