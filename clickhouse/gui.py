import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import clickhouse_connect
import threading


class ClickHouseGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("ClickHouse 数据库管理工具")
        self.root.geometry("1200x750")
        self.root.minsize(900, 600)

        self.client = None
        self.current_db = None

        self._build_ui()
        self._auto_connect()

    def _build_ui(self):
        style = ttk.Style()
        style.configure("Title.TLabel", font=("Microsoft YaHei UI", 11, "bold"))
        style.configure("Status.TLabel", font=("Microsoft YaHei UI", 9))
        style.configure("Treeview", rowheight=28, font=("Microsoft YaHei UI", 9))
        style.configure("Treeview.Heading", font=("Microsoft YaHei UI", 10, "bold"))

        main_frame = ttk.Frame(self.root, padding=8)
        main_frame.pack(fill=tk.BOTH, expand=True)

        top_frame = ttk.LabelFrame(main_frame, text="连接配置", padding=8)
        top_frame.pack(fill=tk.X, pady=(0, 8))

        fields = [
            ("主机地址:", "10.11.22.80", "host"),
            ("端口:", "9120", "port"),
            ("用户名:", "nethouse", "username"),
            ("密码:", "CGC%EVXr.ET10Y_N", "password"),
        ]
        self.entries = {}
        for i, (label_text, default_val, key) in enumerate(fields):
            ttk.Label(top_frame, text=label_text).grid(row=i, column=0, sticky=tk.W, padx=(0, 5), pady=2)
            entry = ttk.Entry(top_frame, width=50, show="*" if key == "password" else "")
            entry.insert(0, default_val)
            entry.grid(row=i, column=1, sticky=tk.EW, padx=(0, 5), pady=2)
            self.entries[key] = entry
        top_frame.columnconfigure(1, weight=1)

        btn_frame = ttk.Frame(top_frame)
        btn_frame.grid(row=len(fields), column=0, columnspan=2, pady=(8, 0))
        ttk.Button(btn_frame, text="连接", command=self._connect, width=12).pack(side=tk.LEFT, padx=4)
        ttk.Button(btn_frame, text="断开", command=self._disconnect, width=12).pack(side=tk.LEFT, padx=4)

        self.status_var = tk.StringVar(value="未连接")
        ttk.Label(top_frame, textvariable=self.status_var, style="Status.TLabel",
                  foreground="gray").grid(row=len(fields), column=1, sticky=tk.E, pady=(8, 0))

        content_frame = ttk.Frame(main_frame)
        content_frame.pack(fill=tk.BOTH, expand=True)

        left_panel = ttk.Frame(content_frame, width=280)
        left_panel.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 8))
        left_panel.pack_propagate(False)

        db_frame = ttk.LabelFrame(left_panel, text="数据库", padding=4)
        db_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 4))

        db_toolbar = ttk.Frame(db_frame)
        db_toolbar.pack(fill=tk.X, pady=(0, 4))
        ttk.Button(db_toolbar, text="刷新", command=self._load_databases, width=8).pack(side=tk.LEFT, padx=2)

        self.db_listbox = tk.Listbox(db_frame, font=("Microsoft YaHei UI", 10), activestyle="none",
                                     selectbackground="#4a90d9", selectforeground="white")
        db_scroll = ttk.Scrollbar(db_frame, orient=tk.VERTICAL, command=self.db_listbox.yview)
        self.db_listbox.configure(yscrollcommand=db_scroll.set)
        db_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.db_listbox.pack(fill=tk.BOTH, expand=True)
        self.db_listbox.bind("<<ListboxSelect>>", self._on_db_select)

        table_frame = ttk.LabelFrame(left_panel, text="数据表", padding=4)
        table_frame.pack(fill=tk.BOTH, expand=True)

        table_toolbar = ttk.Frame(table_frame)
        table_toolbar.pack(fill=tk.X, pady=(0, 4))
        ttk.Button(table_toolbar, text="刷新", command=self._load_tables, width=8).pack(side=tk.LEFT, padx=2)

        self.table_listbox = tk.Listbox(table_frame, font=("Microsoft YaHei UI", 10), activestyle="none",
                                        selectbackground="#4a90d9", selectforeground="white")
        table_scroll = ttk.Scrollbar(table_frame, orient=tk.VERTICAL, command=self.table_listbox.yview)
        self.table_listbox.configure(yscrollcommand=table_scroll.set)
        table_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.table_listbox.pack(fill=tk.BOTH, expand=True)
        self.table_listbox.bind("<<ListboxSelect>>", self._on_table_select)

        right_panel = ttk.Frame(content_frame)
        right_panel.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        field_frame = ttk.LabelFrame(right_panel, text="表字段", padding=4)
        field_frame.pack(fill=tk.X, pady=(0, 4))

        field_cols = ("name", "type", "default_kind", "default_expression", "comment")
        self.field_tree = ttk.Treeview(field_frame, columns=field_cols, show="headings", height=5)
        field_headers = {"name": "字段名", "type": "类型", "default_kind": "默认类型",
                         "default_expression": "默认值", "comment": "备注"}
        col_widths = {"name": 160, "type": 180, "default_kind": 80, "default_expression": 120, "comment": 200}
        for col in field_cols:
            self.field_tree.heading(col, text=field_headers[col])
            self.field_tree.column(col, width=col_widths[col], minwidth=60, anchor=tk.W)
        field_scroll = ttk.Scrollbar(field_frame, orient=tk.VERTICAL, command=self.field_tree.yview)
        self.field_tree.configure(yscrollcommand=field_scroll.set)
        field_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.field_tree.pack(fill=tk.X)

        query_frame = ttk.LabelFrame(right_panel, text="数据查询", padding=4)
        query_frame.pack(fill=tk.BOTH, expand=True)

        sql_bar = ttk.Frame(query_frame)
        sql_bar.pack(fill=tk.X, pady=(0, 4))
        ttk.Label(sql_bar, text="SQL:").pack(side=tk.LEFT)
        self.sql_entry = ttk.Entry(sql_bar)
        self.sql_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(4, 4))
        ttk.Button(sql_bar, text="执行", command=self._execute_query, width=8).pack(side=tk.LEFT, padx=2)
        ttk.Button(sql_bar, text="导出", command=self._export_data, width=8).pack(side=tk.LEFT, padx=2)

        limit_bar = ttk.Frame(query_frame)
        limit_bar.pack(fill=tk.X, pady=(0, 4))
        ttk.Label(limit_bar, text="限制行数:").pack(side=tk.LEFT)
        self.limit_var = tk.StringVar(value="100")
        ttk.Entry(limit_bar, textvariable=self.limit_var, width=8).pack(side=tk.LEFT, padx=(4, 4))

        tree_container = ttk.Frame(query_frame)
        tree_container.pack(fill=tk.BOTH, expand=True)

        self.data_tree = ttk.Treeview(tree_container, show="headings")
        data_scroll_y = ttk.Scrollbar(tree_container, orient=tk.VERTICAL, command=self.data_tree.yview)
        data_scroll_x = ttk.Scrollbar(tree_container, orient=tk.HORIZONTAL, command=self.data_tree.xview)
        self.data_tree.configure(yscrollcommand=data_scroll_y.set, xscrollcommand=data_scroll_x.set)
        data_scroll_y.pack(side=tk.RIGHT, fill=tk.Y)
        data_scroll_x.pack(side=tk.BOTTOM, fill=tk.X)
        self.data_tree.pack(fill=tk.BOTH, expand=True)

        self.result_var = tk.StringVar(value="就绪")
        ttk.Label(query_frame, textvariable=self.result_var, style="Status.TLabel").pack(anchor=tk.W, pady=(4, 0))

    def _auto_connect(self):
        self.root.after(200, self._connect)

    def _get_config(self):
        return {
            "host": self.entries["host"].get().strip(),
            "port": int(self.entries["port"].get().strip()),
            "username": self.entries["username"].get().strip(),
            "password": self.entries["password"].get().strip(),
        }

    def _set_status(self, text, color="gray"):
        self.status_var.set(text)
        colors = {"gray": "gray", "green": "#2e7d32", "red": "#c62828", "blue": "#1565c0"}
        for child in self.root.winfo_children():
            self._apply_status_color(child, colors.get(color, "gray"))

    def _apply_status_color(self, widget, color):
        try:
            if isinstance(widget, ttk.Label) and widget.cget("textvariable") == str(self.status_var):
                widget.configure(foreground=color)
        except Exception:
            pass
        for child in widget.winfo_children():
            self._apply_status_color(child, color)

    def _connect(self):
        try:
            config = self._get_config()
            if self.client:
                self.client.close()
            self.client = clickhouse_connect.get_client(
                host=config["host"],
                port=config["port"],
                secure=True,
                verify=False,
                username=config["username"],
                password=config["password"],
            )
            self._set_status(f"已连接到 {config['host']}:{config['port']}", "green")
            self._load_databases()
        except Exception as e:
            self._set_status(f"连接失败: {e}", "red")
            messagebox.showerror("连接失败", str(e))

    def _disconnect(self):
        if self.client:
            self.client.close()
            self.client = None
            self.current_db = None
        self.db_listbox.delete(0, tk.END)
        self.table_listbox.delete(0, tk.END)
        self._clear_tree(self.field_tree)
        self._clear_tree(self.data_tree)
        self._set_status("已断开", "gray")

    def _load_databases(self):
        if not self.client:
            messagebox.showwarning("提示", "请先连接数据库")
            return
        try:
            result = self.client.query("SHOW DATABASES")
            databases = [row[0] for row in result.result_rows]
            self.db_listbox.delete(0, tk.END)
            for db in databases:
                self.db_listbox.insert(tk.END, db)
            self._set_status(f"已加载 {len(databases)} 个数据库", "blue")
        except Exception as e:
            messagebox.showerror("错误", str(e))

    def _on_db_select(self, event):
        sel = self.db_listbox.curselection()
        if not sel:
            return
        db_name = self.db_listbox.get(sel[0])
        self.current_db = db_name
        self._load_tables()

    def _load_tables(self):
        if not self.client or not self.current_db:
            return
        try:
            result = self.client.query(f"SHOW TABLES FROM `{self.current_db}`")
            tables = [row[0] for row in result.result_rows]
            self.table_listbox.delete(0, tk.END)
            for t in tables:
                self.table_listbox.insert(tk.END, t)
            self._set_status(f"数据库 '{self.current_db}' 包含 {len(tables)} 张表", "blue")
        except Exception as e:
            messagebox.showerror("错误", str(e))

    def _on_table_select(self, event):
        sel = self.table_listbox.curselection()
        if not sel:
            return
        table_name = self.table_listbox.get(sel[0])
        self._load_fields(table_name)
        self._load_table_data(table_name)

    def _load_fields(self, table_name):
        if not self.client or not self.current_db:
            return
        try:
            query = f"SELECT name, type, default_kind, default_expression, comment " \
                    f"FROM system.columns WHERE database = '{self.current_db}' AND table = '{table_name}'"
            result = self.client.query(query)
            self._clear_tree(self.field_tree)
            for row in result.result_rows:
                self.field_tree.insert("", tk.END, values=row)
        except Exception as e:
            messagebox.showerror("错误", str(e))

    def _load_table_data(self, table_name):
        if not self.client or not self.current_db:
            return
        try:
            limit = int(self.limit_var.get())
        except ValueError:
            limit = 100
        sql = f"SELECT * FROM `{self.current_db}`.`{table_name}` LIMIT {limit}"
        self.sql_entry.delete(0, tk.END)
        self.sql_entry.insert(0, sql)
        self._execute_query()

    def _execute_query(self):
        if not self.client:
            messagebox.showwarning("提示", "请先连接数据库")
            return
        sql = self.sql_entry.get().strip()
        if not sql:
            return
        try:
            result = self.client.query(sql)
            columns = result.column_names
            rows = result.result_rows

            self._clear_tree(self.data_tree)
            self.data_tree["columns"] = columns
            for col in columns:
                self.data_tree.heading(col, text=col)
                self.data_tree.column(col, width=120, minwidth=60, anchor=tk.W)

            for row in rows:
                display_row = []
                for val in row:
                    if isinstance(val, (bytes, memoryview)):
                        display_row.append(f"<binary {len(val)}B>")
                    else:
                        display_row.append(str(val) if val is not None else "NULL")
                self.data_tree.insert("", tk.END, values=display_row)

            self.result_var.set(f"查询完成: {len(rows)} 行, {len(columns)} 列")
        except Exception as e:
            self.result_var.set(f"查询失败: {e}")
            messagebox.showerror("查询错误", str(e))

    def _export_data(self):
        if not self.client:
            messagebox.showwarning("提示", "请先连接数据库")
            return
        sql = self.sql_entry.get().strip()
        if not sql:
            messagebox.showwarning("提示", "请先执行查询")
            return
        try:
            result = self.client.query(sql)
            columns = result.column_names
            rows = result.result_rows

            from tkinter import filedialog
            filepath = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("CSV 文件", "*.csv"), ("所有文件", "*.*")],
                title="导出数据"
            )
            if not filepath:
                return

            with open(filepath, "w", encoding="utf-8-sig") as f:
                f.write(",".join(columns) + "\n")
                for row in rows:
                    line_parts = []
                    for val in row:
                        s = str(val) if val is not None else ""
                        if "," in s or '"' in s or "\n" in s:
                            s = '"' + s.replace('"', '""') + '"'
                        line_parts.append(s)
                    f.write(",".join(line_parts) + "\n")

            messagebox.showinfo("导出成功", f"已导出 {len(rows)} 行到:\n{filepath}")
        except Exception as e:
            messagebox.showerror("导出失败", str(e))

    def _clear_tree(self, tree):
        tree.delete(*tree.get_children())
        if tree["columns"]:
            tree["columns"] = ()


def main():
    root = tk.Tk()
    app = ClickHouseGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
