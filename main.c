#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define TABLE_MAX_PAGES 100
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

typedef struct  
{
   int file_descriptor;
   uint32_t file_length;
   uint32_t num_pages;
   void* pages[TABLE_MAX_PAGES];
} Pager;  //页面管理


typedef struct {
   Pager* pager;
   uint32_t root_page_num;
} Table; //表结构

typedef struct {
   uint32_t id;
   char username[COLUMN_USERNAME_SIZE];
   char email[COLUMN_EMAIL_SIZE];
} Row; //行结构，schema

typedef enum {
   STATEMENT_INSERT,
   STATEMENT_SELECT
} StatementType;

typedef struct {
   StatementType type;
   Row row_to_insert;
} Statement; //包含要操作的行和操作类型

typedef struct {
   char* buffer;
   size_t buffer_length;
   ssize_t input_length;
} InputBuffer; //接收用户输入

typedef enum {
   META_COMMAND_SUCCESS,
   META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
   PREPARE_SUCCESS,
   PREPARE_SYNTAX_ERROR,
   PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

typedef enum {
   EXECUTE_SUCCESS,
   EXECUTE_DUPLICATE_KEY,
   EXECUTE_TABLE_FULL
} ExecuteResult;

typedef enum {
   NODE_INTERNAL,
   NODE_LEAF
} NodeType;

typedef struct {
   Table* table;
   uint32_t page_num;
   uint32_t cell_num;
   bool end_of_table;  
} Cursor;

#define ID_SIZE sizeof(((Row*)0)->id)
#define USERNAME_SIZE sizeof(((Row*)0)->username)
#define EMAIL_SIZE sizeof(((Row*)0)->email)
const uint32_t ID_OFFSET=0;
#define USERNAME_OFFSET (ID_OFFSET + ID_SIZE)
#define EMAIL_OFFSET (USERNAME_OFFSET + USERNAME_SIZE)
#define ROW_SIZE (ID_SIZE + USERNAME_SIZE + EMAIL_SIZE)
const uint32_t PAGE_SIZE = 4096; //页大小4KB



/*
   普通节点头部元数据
*/
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
#define IS_ROOT_OFFSET  NODE_TYPE_SIZE
#define PARENT_POINTER_SIZE  sizeof(uint32_t)
#define PARENT_POINTER_OFFSET  (IS_ROOT_OFFSET + IS_ROOT_SIZE)
#define COMMON_NODE_HEADER_SIZE  (NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE)
/*
   叶节点Header信息,每个CELLS是一个键值对
*/
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
#define LEAF_NODE_NUM_CELLS_OFFSET COMMON_NODE_HEADER_SIZE
const uint32_t LEAF_NODE_NEXT_LEAF_SIZE = sizeof(uint32_t);
#define LEAF_NODE_NEXT_LEAF_OFFSET (LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE)
#define LEAF_NODE_HEADER_SIZE (COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE)
/*
   叶节点Body信息，CELLS的value是一个serialized row
*/
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
#define LEAF_NODE_VALUE_OFFSET (LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE)
#define LEAF_NODE_CELL_SIZE  (LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE) //cell大小 = key大小+value大小
#define LEAF_NODE_SPACE_FOR_CELLS  (PAGE_SIZE - LEAF_NODE_HEADER_SIZE)
#define LEAF_NODE_MAX_CELLS (LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE)
#define LEAF_NODE_RIGHT_SPLIT_COUNT  ((LEAF_NODE_MAX_CELLS + 1) / 2)
#define LEAF_NODE_LEFT_SPLIT_COUNT ((LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT) //分裂时N为奇数时，左边多一个
/*
   内部节点Header信息
*/
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
#define INTERNAL_NODE_NUM_KEYS_OFFSET  COMMON_NODE_HEADER_SIZE
const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
#define INTERNAL_NODE_RIGHT_CHILD_OFFSET (INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE)
#define INTERNAL_NODE_HEADER_SIZE (COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE)
/*
   内部节点Body信息
*/
const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
#define INTERNAL_NODE_CELL_SIZE (INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE) 
const uint32_t INTERNAL_NODE_MAX_CELLS = 3;
/*
   访问这个叶节点有多少个cells
*/
uint32_t* leaf_node_num_cells(void* node) {
   return node + LEAF_NODE_NUM_CELLS_OFFSET;
}
/*
   访问这个叶节点的某一个指定的cell
*/
void* leaf_node_cell(void* node, uint32_t cell_num) {
   return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}
/*
   访问这个叶节点的某一个指定的cell的key
*/
uint32_t* leaf_node_key(void* node, uint32_t cell_num) {
   return leaf_node_cell(node, cell_num);
}
/*
   访问这个叶节点的某一个指定的cell的value
*/
void* leaf_node_value(void* node,uint32_t cell_num) {
   return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}
/*
   访问这个叶节点的next指针
*/
uint32_t* leaf_node_next_leaf(void* node) {
   return node + LEAF_NODE_NEXT_LEAF_OFFSET;
}

bool is_node_root(void* node) {
   uint8_t value = *((uint8_t*)(node + IS_ROOT_OFFSET));
   return (bool)value;
}

void set_node_root(void* node, bool is_root) {
   uint8_t value = is_root;
   *((uint8_t*)(node + IS_ROOT_OFFSET)) = value; 
}
/*
   初始化叶节点
*/
void initialize_leaf_node(void* node) {
   set_node_type(node, NODE_LEAF);
   set_node_root(node, false);
   *leaf_node_num_cells(node) = 0;
   *leaf_node_next_leaf(node) = 0;
}

uint32_t* internal_node_num_keys(void* node) {
   return node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}

/*
   初始化内部节点
*/
void initialize_internal_node(void* node) {
   set_node_type(node, NODE_INTERNAL);
   set_node_root(node, false);
   *internal_node_num_keys(node) = 0;
}

uint32_t* internal_node_right_child(void* node) {
   return node + INTERNAL_NODE_RIGHT_CHILD_OFFSET;
}

uint32_t* internal_node_cell(void* node, uint32_t cell_num) {
   return node + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE;
}

uint32_t* internal_node_child(void* node, uint32_t child_num) {
   uint32_t num_keys = *internal_node_num_keys(node);
   if (child_num > num_keys) {
      printf("尝试访问child_num %d > num_keys %d\n", child_num, num_keys);
      exit(EXIT_FAILURE);
   } else if (child_num == num_keys) {
      return internal_node_right_child(node);
   } else {
      return internal_node_cell(node, child_num);
   }
}

uint32_t* internal_node_key(void* node, uint32_t key_num) {
   return (void*)internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
}

//内存中新建一个接收输入的缓冲区
InputBuffer* new_input_buffer() {
   InputBuffer* input_buffer = (InputBuffer*)malloc(sizeof(InputBuffer));
   input_buffer->buffer = NULL;
   input_buffer->buffer_length = 0;
   input_buffer->input_length = 0;

   return input_buffer;
}

//打印前缀db >
void print_prompt() { printf("db >");}


ssize_t getline(char **lineptr, size_t *n, FILE *stream);

//接收用户输入
void read_input(InputBuffer* input_buffer) {
   ssize_t bytes_read = 
      getline(&(input_buffer->buffer), &(input_buffer->buffer_length) ,stdin);
   
   if(bytes_read <= 0) {
      printf("Error reading input\n");
      exit(EXIT_FAILURE);
   }

   //忽略输入的换行符
   input_buffer->input_length = bytes_read -1;
   input_buffer->buffer[bytes_read - 1] = 0;
}

void close_input_buffer(InputBuffer* input_buffer) {
   free(input_buffer->buffer);
   free(input_buffer);
}


Pager* pager_open(const char* filename) {
   int fd = open(filename,
               O_RDWR|O_CREAT,
               S_IWUSR|S_IRUSR);
   if (fd == -1) {
      printf("Unable to open file\n");
      exit(EXIT_FAILURE);
   }

   off_t file_length = lseek(fd, 0, SEEK_END); //移到文件末尾，返回文件偏移量

   Pager* pager = malloc(sizeof(Pager));
   pager->file_descriptor = fd;
   pager->file_length = file_length;
   pager->num_pages = (file_length / PAGE_SIZE);

   if(file_length % PAGE_SIZE !=0) {
      printf("Db file is not a whole number of pages. Corrupt file.(页大小4kb,db 文件不是4的整数倍)\n");
      exit(EXIT_FAILURE);
   } 

   for (uint32_t i = 0;i < TABLE_MAX_PAGES; i++) {
      pager->pages[i] = NULL;
   }

   return pager;
}

void print_row(Row* row) {
 printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

void* get_page(Pager* pager,uint32_t page_num) {
   if (page_num > TABLE_MAX_PAGES) {
      printf("Tried to fetch page number out of bounds. %d > %d\n",page_num,TABLE_MAX_PAGES);
      exit(EXIT_FAILURE);
   }

   if (pager->pages[page_num] == NULL) {
      // 缓存未命中，分配内存并从磁盘加载
      void* page = malloc(PAGE_SIZE);
      uint32_t num_pages = pager->file_length / PAGE_SIZE;
      if (pager->file_length % PAGE_SIZE) {
         num_pages +=1;
      }

      if (page_num <= num_pages) {
         lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
         ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
         if (bytes_read == -1) {
            printf("Error reading file: %d\n", errno);
            exit(EXIT_FAILURE);
         }
      }

      pager->pages[page_num] = page;

      if(page_num >= pager->num_pages) {
         pager->num_pages = page_num + 1;
      }
   }

   return pager->pages[page_num];
}

//打开一个db文件并跟踪其大小，并且初始化pager和table
Table* db_open(const char* filename) {
   Pager* pager = pager_open(filename);
   
   Table* table = (Table*)malloc(sizeof(Table));
   table->pager = pager;
   table->root_page_num = 0;

   if (pager->num_pages == 0) {
      //新数据库文件，初始化page0为叶节点
      void* root_node = get_page(pager, 0);
      initialize_leaf_node(root_node);
      set_node_root(root_node, true);
   }
   return table;
}

//返回一个指针，指向cursor所指的行
void* cursor_value(Cursor* cursor) {
   uint32_t page_num = cursor->page_num;
   void* page = get_page(cursor->table->pager, page_num);
   return leaf_node_value(page, cursor->cell_num);
}

//检测insert 还是 select ，并判断语法
PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement) {
   if (strncmp(input_buffer->buffer, "insert", 6)==0) {
      statement->type = STATEMENT_INSERT;
      int args_assigned = sscanf(input_buffer->buffer, "insert %d %s %s", &(statement->row_to_insert.id),statement->row_to_insert.username, statement->row_to_insert.email);
      if (args_assigned < 3) {
         return PREPARE_SYNTAX_ERROR;
      }
      return PREPARE_SUCCESS;
   }
   if(strcmp(input_buffer->buffer, "select") == 0) {
      statement->type = STATEMENT_SELECT;
      return PREPARE_SUCCESS;
   }

   return PREPARE_UNRECOGNIZED_STATEMENT;
}

void serialize_row(Row* source, void* destination) {
   memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
   memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
   memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);  
}

void deserialize_row(void *source, Row* destination) {
  memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
  memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
  memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

void pager_flush(Pager* pager, uint32_t page_num) {
   if (pager->pages[page_num] == NULL) {
      printf("Tried to flush null page\n");
      exit(EXIT_FAILURE);
   }

   ssize_t bytes_written = 
         write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);//将pages字符串写入文件

   if (bytes_written == -1) {
      printf("Error writing: %d\n", errno);
      exit(EXIT_FAILURE);
   }
}

void db_close(Table* table) {
   Pager* pager = table->pager;
   
   for (uint32_t i = 0;i< pager->num_pages; i++) {
      if (pager->pages[i] == NULL) {
         continue;
      }
      pager_flush(pager, i);
      free(pager->pages[i]);
      pager->pages[i] == NULL;
   } 

   int result = close(pager->file_descriptor);
   if (result == -1) {
      printf("Error closing db file.\n");
      exit(EXIT_FAILURE);
   }
   for (uint32_t i = 0; i< TABLE_MAX_PAGES; i++) {
      void* page = pager->pages[i];
      if (page) {
         free(page);
         pager->pages[i] = NULL;
      } 
   }
   free(pager);
   free(table);
}

void print_constants() {
   printf("ROW_SIZE: %d\n", ROW_SIZE);
   printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
   printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
   printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
   printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
   printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

void printf_leaf_node(void* node) {
   uint32_t num_cells = *leaf_node_num_cells(node);
   printf("leaf (size %d)\n", num_cells);
   for (uint32_t i = 0;i < num_cells; i++) {
      uint32_t key = *leaf_node_key(node, i);
      printf("  - %d : %d\n", i, key);
   }
}

//进行exit等一些其他操作
MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* table) {
   if (strcmp(input_buffer->buffer, ".exit") == 0) {
      db_close(table);
      exit(EXIT_SUCCESS);
   }
   else if (strcmp(input_buffer->buffer, ".btree") == 0){
      printf("Tree:\n");
      print_tree(table->pager, 0, 0);
      return META_COMMAND_SUCCESS;
   }
   else if (strcmp(input_buffer->buffer, ".constants") == 0){
      printf("Constants:\n");
      print_constants();
      return META_COMMAND_SUCCESS;
   }
   else {
      return META_COMMAND_UNRECOGNIZED_COMMAND;
   }
}

Cursor* internal_node_find_child(void* node, uint32_t key) {
   uint32_t num_keys = *internal_node_num_keys(node);

   /*
      二分搜索
   */
  uint32_t min_index = 0;
  uint32_t max_index = num_keys;

  while(min_index != max_index) {
   uint32_t index = (min_index + max_index) / 2;
   uint32_t key_to_right = *internal_node_key(node, index);
   if (key_to_right >= key) {
      max_index = index;
   } else {
      min_index = index + 1;
   }
  }

  return min_index;
}

Cursor* leaf_node_find(Table* table, uint32_t page_num, uint32_t key) {
   void* node = get_page(table->pager, page_num);
   uint32_t num_cells = *leaf_node_num_cells(node);

   Cursor* cursor = malloc(sizeof(Cursor));
   cursor->table = table;
   cursor->page_num = page_num;

   //二分查找
   uint32_t min_index = 0;
   uint32_t one_past_max_index = num_cells;
   while (one_past_max_index != min_index) {
      uint32_t index = (min_index + one_past_max_index) / 2;
      uint32_t key_at_index = *leaf_node_key(node, index);
      if (key == key_at_index) {
         cursor->cell_num = index;
         return cursor;//找到，返回cell位置
      }
      if (key < key_at_index) {
         one_past_max_index = index;
      } else {
         min_index = index + 1;
      }

      cursor->cell_num = min_index;
      return cursor;
   }
}

NodeType get_node_type(void* node) {
   uint8_t value = *((uint8_t*)(node + NODE_TYPE_OFFSET));
   return (NodeType)value;
}

Cursor* internal_node_find(Table* table, uint32_t page_num, uint32_t key) {
  void* node = get_page(table->pager, page_num);
   /*
      找到后的子节点可能是叶节点也可能是内部节点
   */
  uint32_t child_index = internal_node_find_child(node, key);
  uint32_t child_num = *internal_node_child(node, child_index);
  void* child = get_page(table->pager, child_num);
  switch (get_node_type(child)) {
      case NODE_LEAF:
         return leaf_node_find(table, child_num, key);
      case NODE_INTERNAL:
         return internal_node_find(table, child_num, key);
  }
}

/*
   返回给定key的位置，如果key不存在，返回它应该插入的位置
*/
Cursor* table_find(Table* table, uint32_t key) {
   uint32_t root_page_num = table->root_page_num;
   void* root_node = get_page(table->pager, root_page_num);

   if (get_node_type(root_node) == NODE_LEAF) {
      return leaf_node_find(table, root_page_num, key); //是叶节点，在节点里查找
   } else {
      return internal_node_find(table, root_page_num, key);
   }
}

//生成一个指向表头的光标
Cursor* table_start(Table* table) {
   Cursor* cursor = table_find(table, 0);

   void* node = get_page(table->pager, cursor->page_num);
   uint32_t num_cells = *leaf_node_num_cells(node);
   cursor->end_of_table = (num_cells == 0);

   return cursor;
}

void set_node_type(void* node, NodeType type) {
   uint8_t value = type;
   *((uint8_t*)(node + NODE_TYPE_OFFSET)) = value;
}
/*
   返回该节点的父节点
*/
uint32_t* node_parent(void* node) {
   return node + PARENT_POINTER_OFFSET;
}

void update_internal_node_key(void* node, uint32_t old_key, uint32_t new_key) {
   uint32_t old_child_index = internal_node_find_child(node, old_key);
   *internal_node_key(node, old_child_index) = new_key;
}

uint32_t get_node_max_key(void* node) {
   switch (get_node_type(node)) {
      case NODE_INTERNAL:
         return *internal_node_key(node, *internal_node_num_keys(node) - 1);
      case NODE_LEAF:
         return *leaf_node_key(node, *leaf_node_num_cells(node) - 1);
   }
}

void internal_node_insert(Table* table, uint32_t parent_page_num, uint32_t child_page_num) {
  /*
  Add a new child/key pair to parent that corresponds to child
  */
  
  void* parent = get_page(table->pager, parent_page_num);
  void* child = get_page(table->pager, child_page_num);
  uint32_t child_max_key = get_node_max_key(child);
  uint32_t index = internal_node_find_child(parent, child_max_key);

  uint32_t original_num_keys = *internal_node_num_keys(parent);
  *internal_node_num_keys(parent) = original_num_keys + 1;

  if (original_num_keys >= INTERNAL_NODE_MAX_CELLS) {
      printf("这里需要引入分裂内部节点\n");
      exit(EXIT_FAILURE);
  }

  uint32_t right_child_page_num = *internal_node_right_child(parent);
  void* right_child = get_page(table->pager, right_child_page_num);

  if (child_max_key > get_node_max_key(right_child)) {
    /*
      replace right child
    */
    *internal_node_child(parent, original_num_keys) = right_child_page_num;
    *internal_node_key(parent, original_num_keys) = get_node_max_key(right_child);
    *internal_node_right_child(parent) = child_page_num;
  } else {
    /*
      make room for the new cell
    */
    for(uint32_t i = original_num_keys; i > index; i--) {
      void* destination = internal_node_cell(parent, i);
      void* source = internal_node_cell(parent, i - 1);
      memcpy(destination, source, INTERNAL_NODE_CELL_SIZE);
    }
    *internal_node_child(parent, index) = child_page_num;
    *internal_node_key(parent, index) = child_max_key;
  }
}

//光标前进一行
void cursor_advance(Cursor* cursor) {
   uint32_t page_num = cursor->page_num;
   void* node = get_page(cursor->table->pager, page_num);

   cursor->cell_num +=1;
   if(cursor->cell_num >= (*leaf_node_num_cells(node))) {
      /*前往下一个叶节点*/
      uint32_t next_page_num = *leaf_node_next_leaf(node);
      if (next_page_num == 0) {
         /* 这是最右边的叶节点了*/
         cursor->end_of_table = true;
      } else {
         cursor->page_num = next_page_num;
         cursor->cell_num = 0;
      }
   }
      
}

//执行insert
/*
   part9：插入改为顺序插入，而不是始终插入到表尾
*/
ExecuteResult execute_insert(Statement* statement,Table* table) {
   void* node = get_page(table->pager, table->root_page_num);
   uint32_t num_cells = (*leaf_node_num_cells(node));
   // if (num_cells >= LEAF_NODE_MAX_CELLS) {
   //    return EXECUTE_TABLE_FULL;
   // }

   Row* row_to_insert = &(statement->row_to_insert); //获取statement里要插入的row
   uint32_t key_to_insert = row_to_insert->id; //按id排序
   Cursor* cursor = table_find(table, key_to_insert);

   if(cursor->cell_num <num_cells) {
      uint32_t key_at_index = *leaf_node_key(node, cursor->cell_num);
      if (key_at_index == key_to_insert) {
         return EXECUTE_DUPLICATE_KEY;
      } //插入了重复行
   }

   leaf_node_insert(cursor, row_to_insert->id, row_to_insert);

   free(cursor);

   return EXECUTE_SUCCESS;
}

//执行select，光标指向表头，while读取直至表尾
ExecuteResult execute_select(Statement* statement, Table* table) {
   Cursor* cursor = table_start(table);
   Row row;
   
   while (!(cursor->end_of_table)) {
      deserialize_row(cursor_value(cursor), &row); //内容拷贝到row
      print_row(&row); 
      cursor_advance(cursor); //光标前进一行
   }

   free(cursor);

   return EXECUTE_SUCCESS;
}

//根据状态选择对表的操作
ExecuteResult execute_statement(Statement* statement, Table* table) {
   switch (statement->type) {
      case (STATEMENT_INSERT):
         return execute_insert(statement, table);
      case (STATEMENT_SELECT) :
         return execute_select(statement, table);
   }
}

/*
   实现空闲页面回收之前，目前新页面总是会加入到数据库文件末尾
*/
uint32_t get_unused_page_num(Pager* pager) {
   return pager->num_pages;
}

void create_new_root(Table* table,uint32_t right_child_page_num) {
   /*
     Handle splitting the root.
     Old root copied to new page, becomes left child.
     Address of right child passed in.
     Re-initialize root page to contain the new root node.
     New root node points to two children.
   */
   void* root = get_page(table->pager, table->root_page_num);
   void* right_child = get_page(table->pager, right_child_page_num);
   uint32_t left_child_page_num = get_unused_page_num(table->pager);
   void* left_child = get_page(table->pager, left_child_page_num);
   /*
      旧的根节点数据被复制到左子节点
   */
   memcpy(left_child, root, PAGE_SIZE);
   set_node_root(left_child, false);
   /*
      Root node is a new internal node with one key and two children
   */
   initialize_internal_node(root);
   set_node_root(root, true);
   *internal_node_num_keys(root) = 1;
   *internal_node_child(root, 0) = left_child_page_num;
   uint32_t left_child_max_key = get_node_max_key(left_child);
   *internal_node_key(root, 0) = left_child_max_key;
   *internal_node_right_child(root) = right_child_page_num;
   *node_parent(left_child) = table->root_page_num;
   *node_parent(right_child) = table->root_page_num;
}

void leaf_node_split_and_insert(Cursor* cursor, uint32_t key, Row* value) {
  /*
  Create a new node and move half the cells over.
  Insert the new value in one of the two nodes.
  Update parent or create a new parent.
  */
   void* old_node = get_page(cursor->table->pager, cursor->page_num);   
   uint32_t old_max = get_node_max_key(old_node);
   uint32_t new_page_num = get_unused_page_num(cursor->table->pager);
   void* new_node = get_page(cursor->table->pager, new_page_num);
   initialize_leaf_node(new_node);
   *leaf_node_next_leaf(new_node) = *leaf_node_next_leaf(old_node);
   *leaf_node_next_leaf(old_node) = new_page_num;
  /*
  All existing keys plus new key should be divided
  evenly between old (left) and new (right) nodes.
  Starting from the right, move each key to correct position.
  */
   for (int32_t i = LEAF_NODE_MAX_CELLS; i >= 0; i--) {
      void* destination_node;
      if (i >= LEAF_NODE_LEFT_SPLIT_COUNT) {
         destination_node = new_node;
      } else {
         destination_node = old_node;
      }
      uint32_t index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
      void* destination = leaf_node_cell(destination_node, index_within_node);

      if (i == cursor->cell_num) {
         serialize_row(value, leaf_node_value(destination_node, index_within_node));
         *leaf_node_key(destination_node, index_within_node) = key;
      } else if (i > cursor->cell_num) {
         memcpy(destination, leaf_node_cell(old_node, i-1), LEAF_NODE_CELL_SIZE);
      } else {
         memcpy(destination, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE);
      }
   }
   /*
      更新两个叶节点的节点数量
   */
  *(leaf_node_num_cells(old_node)) = LEAF_NODE_LEFT_SPLIT_COUNT;
  *(leaf_node_num_cells(new_node)) = LEAF_NODE_RIGHT_SPLIT_COUNT;

  if (is_node_root(old_node)) { 
      return create_new_root(cursor->table, new_page_num); //原始节点是根节点
  } else {
      uint32_t parent_page_num = *node_parent(old_node);
      uint32_t new_max = get_node_max_key(old_node);
      void* parent = get_page(cursor->table->pager, parent_page_num);

      update_internal_node_key(parent, old_max, new_max);
      internal_node_insert(cursor->table, parent_page_num, new_page_num);
      return;
  }
}

void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value) {
   void* node = get_page(cursor->table->pager, cursor->page_num);

   uint32_t num_cells = *leaf_node_num_cells(node);
   if (num_cells >= LEAF_NODE_MAX_CELLS) {
      //节点满了
      leaf_node_split_and_insert(cursor, key, value);
      return;
   }

   if (cursor->cell_num < num_cells) {
      //为新cell分配空间，所有cell前移一个单位
      for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
         memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i-1), LEAF_NODE_CELL_SIZE);
      }
   }

   *(leaf_node_num_cells(node)) += 1;
   *(leaf_node_key(node, cursor->cell_num)) = key;
   serialize_row(value, leaf_node_value(node, cursor->cell_num));
}

void indent(uint32_t level) {
   for (uint32_t i = 0; i < level; i++) {
      printf("  ");
   }
}

void print_tree(Pager* pager, uint32_t page_num, uint32_t indentation_level) {
   void* node = get_page(pager, page_num);
   uint32_t num_keys, child;

   switch (get_node_type(node)) {
      case (NODE_LEAF):
         num_keys = *leaf_node_num_cells(node);
         indent(indentation_level);
         printf("- leaf (size %d)\n", num_keys);
         for (uint32_t i = 0; i < num_keys; i++) {
            indent(indentation_level + 1);
            printf("- %d\n", *leaf_node_key(node, i));
         }
         break;
      case (NODE_INTERNAL):
         num_keys = *internal_node_num_keys(node);
         indent(indentation_level);
         printf("- internal (size %d)\n", num_keys);
         for (uint32_t i = 0; i < num_keys; i++) {
            child = *internal_node_child(node, i);
            print_tree(pager, child, indentation_level + 1);

            indent(indentation_level + 1);
            printf("- key %d\n", *internal_node_key(node, i));
         }
         child = *internal_node_right_child(node);
         print_tree(pager, child, indentation_level + 1);
         break;
   }
}

int main(int argc, char* argv[])
{
   if (argc < 2) {
      printf("Must supply a database filename.\n");
      exit(EXIT_FAILURE);
   }

   char* filename = argv[1];
   Table* table = db_open(filename);
   InputBuffer* input_buffer = new_input_buffer();
   while(true){
      print_prompt();
      read_input(input_buffer);

      if(input_buffer->buffer[0] == '.') {
         switch ( do_meta_command(input_buffer, table))
         {
            case (META_COMMAND_SUCCESS):
               continue;
            case (META_COMMAND_UNRECOGNIZED_COMMAND):
               printf("Unrecognized command '%s'\n", input_buffer->buffer);
               continue;
         }
      }
      
      Statement statement;
      switch (prepare_statement(input_buffer, &statement))
      {
         case (PREPARE_SUCCESS):
            break;
         case (PREPARE_SYNTAX_ERROR):
            printf("Syntax error. Could not parse statement.\n");
            continue;
         case (PREPARE_UNRECOGNIZED_STATEMENT):
            printf("Unrecognized keyword at start of '%s'.\n",input_buffer->buffer);
            continue;
      }

      switch (execute_statement(&statement, table)) {
         case (EXECUTE_SUCCESS):
            printf("Executed.\n");
            break;
         case (EXECUTE_DUPLICATE_KEY):
            printf("Error: Duplicate Key.\n");
            break;
         case (EXECUTE_TABLE_FULL):
            printf("Error: Table full.\n");
            break;
      }
   }
 
   return 0;
}