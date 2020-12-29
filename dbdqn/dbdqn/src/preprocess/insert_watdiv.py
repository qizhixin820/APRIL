def init_watdiv_t0(self, filepath):
        with open(filepath) as fp:
            text_lines = fp.readlines()
        fp.close()
        try:
            self.cur.execute(
                "create table if not exists t0 (p varchar(255) not null, s varchar(255) not null, o varchar(255) not null) charset utf8")
            self.conn.commit()
        except:
            self.conn.rollback()
        data = list()
        print(len(text_lines))

        for line in text_lines:
            match_line = re.match('<(.*)>\\s*<(.*)>\\s*(<.*>|".*")\\s*\\.', line)
            if match_line:
                s = match_line.group(1)
                p = match_line.group(2)
                o = match_line.group(3)[1:-1][:255]
                data.append((p, s, o))
            else:
                print(line)
        data = tuple(data)
        sql = "INSERT INTO t0(p, s, o) VALUES (%s, %s, %s)"
        patch = 10000
        for i in range(len(data) // patch + 1):
            print(i * patch, ":", (i + 1) * patch)
            self.cur.executemany(sql, data[i * patch:(i + 1) * patch])
        self.conn.commit()