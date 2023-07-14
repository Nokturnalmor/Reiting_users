from isdayoff import ProdCalendar
import datetime
import asyncio
import project_cust_38.Cust_Functions as F
import project_cust_38.Cust_SQLite as CSQ
import copy
#F.test_path()

put_db = F.bdcfg('BD_users')
put_emploee = F.tcfg('employee')
db_kplan = F.bdcfg('DB_kplan')
plecho = 3


if not F.nalich_file(put_db):
    frase = "VACUUM;"
    CSQ.sozd_bd_sql(put_db, frase)
    print('Создана вновь')
else:
    print('На месте')

first_day_1 = datetime.datetime.today().replace(day=1).date()
months = [first_day_1]
for i in range(1, plecho + 1):
    months.append(F.add_months(first_day_1, i).date())
calendar = ProdCalendar(locale='ru')

def add_jurnal_kplan(dict_podr, res, ima_table_jurnal_kplan,spis_empl):
    def count_empl(podr,spis_empl):
        count = 0
        for i in range(len(spis_empl)):
            if podr in spis_empl[i][4]:
                count+=1
        return count

    list_podr_range = []
    for i in range(len(dict_podr)):
        for name in dict_podr.keys():
            if dict_podr[name]['Порядок'] == i:
                list_podr_range.append(name)
    cols = []
    cols_stat = ['', 'Выходные']
    cols_dn = ['podr', 'День недели']
    row_empl = []

    for i in range(len(list_podr_range)):
        row_empl.append([])
        row_empl[-1].append(list_podr_range[i])
        row_empl[-1].append(count_empl(dict_podr[list_podr_range[i]]['Наименование'],spis_empl))

    for i in res:
        cols.append('d_' + i.replace('.', '_'))
        cols_stat.append(res[i].value)
        cols_dn.append(F.strtodate(i, '%Y.%m.%d').weekday() + 1)
        for podr in row_empl:
            if res[i].value == 1:
                podr.append(0)
            else:
                podr.append(8 * podr[1])
    text = ' INTEGER, '.join(cols)
    frase_tmp = f"""CREATE TABLE IF NOT EXISTS {ima_table_jurnal_kplan}(
                           Пномер INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE ON CONFLICT ROLLBACK,
                           Подразделение TEXT,
                            Примечание TEXT,
                           {text} INTEGER);
                        """
    CSQ.sozd_bd_sql(db_kplan, frase_tmp)
    CSQ.dob_strok_v_bd_sql(db_kplan, ima_table_jurnal_kplan, [cols_stat])
    CSQ.dob_strok_v_bd_sql(db_kplan, ima_table_jurnal_kplan, [cols_dn])
    CSQ.dob_strok_v_bd_sql(db_kplan, ima_table_jurnal_kplan, row_empl)
    print(f'    Добавлен {ima_table_jurnal_kplan}')


def add_tbl_empl(spis_empl,res,ima_table_empl,conn,dict_rab_vrema):
    cols = []
    cols_stat = ['', 'Выходные']
    cols_dn = ['ФИО', 'День недели']
    row_empl = []
    for i in range(3,len(spis_empl)):
        row_empl.append([])
        row_empl[-1].append(f"{spis_empl[i][1]} {spis_empl[i][2]}")
        row_empl[-1].append('')
    for i in res:
        cols.append('d_' + i.replace('.', '_'))
        cols_stat.append(res[i].value)
        cols_dn.append(F.strtodate(i, '%Y.%m.%d').weekday() + 1)
        for rc in row_empl:
            if res[i].value == 1:
                rc.append(0)
            else:
                norma = 8
                if ' '.join(rc[0].split()[:3]) in dict_rab_vrema:
                    norma = dict_rab_vrema[' '.join(rc[0].split()[:3])]
                rc.append(norma)
    text = ' INTEGER, '.join(cols)
    frase_tmp = f"""CREATE TABLE IF NOT EXISTS {ima_table_empl}(
                       Пномер INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE ON CONFLICT ROLLBACK,
                       ФИО TEXT,
                        Примечание TEXT,
                       {text} INTEGER);
                    """
    CSQ.sozd_bd_sql(put_db, frase_tmp, conn=conn)
    CSQ.dob_strok_v_bd_sql(put_db, ima_table_empl, [cols_stat], conn=conn)
    CSQ.dob_strok_v_bd_sql(put_db, ima_table_empl, [cols_dn], conn=conn)
    CSQ.dob_strok_v_bd_sql(put_db, ima_table_empl, row_empl, conn=conn)
    print(f'    Добавлен {ima_table_empl}')


def add_tbl_jurnaltdz(list_rc,res,ima_table_empl,conn):
    cols = []
    cols_stat = ['', 'Выходные']
    cols_dn = ['РЦ', 'Отв./День недели']
    row_rc = []
    for rc in list_rc.keys():
        if rc[:3] == '010' and rc[-2:] == '00' and '0000' not in rc:
            row_rc.append([])
            row_rc[-1].append(rc)
            row_rc[-1].append(list_rc[rc]['Отв_мастер_тдз'])
    for i in res:
        cols.append('d_' + i.replace('.', '_'))
        cols_stat.append(res[i].value)
        cols_dn.append(F.strtodate(i, '%Y.%m.%d').weekday() + 1)
        for rc in row_rc:
            if res[i].value == 1:
                rc.append(1)
            else:
                rc.append(0)
    text = ' INTEGER, '.join(cols)
    frase_tmp = f"""CREATE TABLE IF NOT EXISTS {ima_table_empl}(
                       Пномер INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE ON CONFLICT ROLLBACK,
                       РЦ TEXT,
                        Примечание TEXT,
                       {text} INTEGER);
                    """
    CSQ.sozd_bd_sql(put_db, frase_tmp, conn=conn)
    CSQ.dob_strok_v_bd_sql(put_db, ima_table_empl, [cols_stat], conn=conn)
    CSQ.dob_strok_v_bd_sql(put_db, ima_table_empl, [cols_dn], conn=conn)
    CSQ.dob_strok_v_bd_sql(put_db, ima_table_empl, row_rc, conn=conn)
    print(f'    Добавлен {ima_table_empl}')

def add_tbl_eq(row_equip, res, ima_table_eq, conn):
    row_equip_tmp = copy.deepcopy(row_equip)
    cols = []
    cols_stat = ['', 'Выходные']
    cols_dn = ['Пномер_оборудования', 'День недели']
    for i in res:
        cols.append('d_' + i.replace('.', '_'))
        cols_stat.append(res[i].value)
        cols_dn.append(F.strtodate(i, '%Y.%m.%d').weekday() + 1)
        for rc in row_equip_tmp:
            kol_vo = rc[0]
            if res[i].value == 1:
                rc.append(24*kol_vo)
            else:
                rc.append(24*kol_vo)
    for rc in row_equip_tmp:
        rc.pop(0)
    text = ' INTEGER, '.join(cols)
    frase_tmp = f"""CREATE TABLE IF NOT EXISTS {ima_table_eq}(
                       Пномер INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE ON CONFLICT ROLLBACK,
                       Пномер_оборудования INTEGER,
                        Примечание TEXT,
                       {text} INTEGER);
                    """
    CSQ.sozd_bd_sql(put_db, frase_tmp, conn=conn)
    CSQ.dob_strok_v_bd_sql(put_db, ima_table_eq, [cols_stat], conn=conn)
    CSQ.dob_strok_v_bd_sql(put_db, ima_table_eq, [cols_dn], conn=conn)
    CSQ.dob_strok_v_bd_sql(put_db, ima_table_eq, row_equip_tmp, conn=conn)
    print(f'    Добавлен {ima_table_eq}')

def add_tbl_rm(row_rm, res, ima_table_rm, conn):
    row_rm_tmp = copy.deepcopy(row_rm)
    cols = []
    cols_stat = ['', 'Выходные']
    cols_dn = ['Пномер_рм', 'День недели']
    for i in res:
        cols.append('d_' + i.replace('.', '_'))
        cols_stat.append(res[i].value)
        cols_dn.append(F.strtodate(i, '%Y.%m.%d').weekday() + 1)
        for rc in row_rm_tmp:
            if res[i].value == 1:
                rc.append(96)
            else:
                rc.append(96)
    text = ' INTEGER, '.join(cols)
    frase_tmp = f"""CREATE TABLE IF NOT EXISTS {ima_table_rm}(
                       Пномер INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE ON CONFLICT ROLLBACK,
                       Пномер_рм INTEGER,
                        Примечание TEXT,
                       {text} INTEGER);
                    """
    CSQ.sozd_bd_sql(put_db, frase_tmp, conn=conn)
    CSQ.dob_strok_v_bd_sql(put_db, ima_table_rm, [cols_stat], conn=conn)
    CSQ.dob_strok_v_bd_sql(put_db, ima_table_rm, [cols_dn], conn=conn)
    CSQ.dob_strok_v_bd_sql(put_db, ima_table_rm, row_rm_tmp, conn=conn)
    print(f'    Добавлен {ima_table_rm}')

def reload_tbl_empl(ima_table_empl,spis_empl,res,dict_rab_vrema):
    zapros = f'SELECT ФИО FROM {ima_table_empl} WHERE ФИО != "" AND Пномер > 2'
    rez = CSQ.zapros(put_db, zapros, shapka=False)
    rez = [i[0] for i in rez]
    strok_rez = []
    if F.strtodate(ima_table_empl,f"{ima_table_empl.split('_')[0]}_%Y_%m_%d") >= F.nach_kon_date(F.strtodate(F.now()),vid='m',format_in='',format_out='')[0]:
        for i in range(len(rez)):
            fio = ' '.join(rez[i].split()[:3])
            dolgn = ' '.join(rez[i].split()[3:])
            for j in range(2, len(spis_empl)):
                if spis_empl[j][1] == fio and spis_empl[j][2] != dolgn and spis_empl[j][3] == '':
                    CSQ.zapros(put_db,f"""UPDATE {ima_table_empl} SET ФИО = '{spis_empl[j][1]} {spis_empl[j][2]}' WHERE ФИО == '{rez[i]}';""")
                    rez[i] = f'{spis_empl[j][1]} {spis_empl[j][2]}'
    for i in range(2,len(spis_empl)):
        if spis_empl[i][3] != '':
            continue
        if f"{spis_empl[i][1]} {spis_empl[i][2]}" not in rez:
            primech = ""
            if spis_empl[i][3] ==  'Увольнение':
                primech = 'Увольнение'
                norma = 0
            strok = [f"{spis_empl[i][1]} {spis_empl[i][2]}", primech]
            fio = spis_empl[i][1]
            tek_dat = F.now()
            for i in res:
                if F.strtodate(i, "%Y.%m.%d") <= F.strtodate(tek_dat):
                    strok.append(0)
                else:
                    if res[i].value == 1:
                        strok.append(0)
                    else:
                        norma = 8
                        if fio in dict_rab_vrema:
                            norma = dict_rab_vrema[fio]
                        strok.append(norma)
            strok_rez.append(strok)

    if len(strok_rez)>0:
        CSQ.dob_strok_v_bd_sql(put_db, ima_table_empl, strok_rez)


def reload_tbl_eq(ima_table_eq, row_equip, res):
    row_equip_tmp = copy.deepcopy(row_equip)
    print(f'    {ima_table_eq} на месте')
    print(f'     обновление')
    zapros = f'SELECT Пномер_оборудования FROM {ima_table_eq} WHERE Пномер_оборудования != "" AND Пномер > 2'
    rez = CSQ.zapros(put_db, zapros, shapka=False)
    rez = [i[0] for i in rez]
    strok_rez = []
    for i in range(len(row_equip_tmp)):
        if row_equip_tmp[i][1] not in rez:
            strok = [row_equip_tmp[i][1], '']
            tek_dat = F.now()
            for day in res:
                if F.strtodate(day, "%Y.%m.%d") <= F.strtodate(tek_dat):
                    strok.append(0)
                else:
                    strok.append(24*row_equip_tmp[i][0])
            strok_rez.append(strok)
    CSQ.dob_strok_v_bd_sql(put_db, ima_table_eq, strok_rez)


def reload_tbl_jurnaltdz(list_podr, ima_table_jurnaltdz):
    zapros = f'SELECT * FROM {ima_table_jurnaltdz}'
    rez = CSQ.zapros(put_db, zapros, rez_dict=True)
    for i in range(len(rez)):
        if rez[i]['РЦ'] in list_podr:
            if rez[i]['Примечание'] != list_podr[rez[i]['РЦ']]['Отв_мастер_тдз']:
                CSQ.zapros(put_db,f"""UPDATE {ima_table_jurnaltdz} SET Примечание 
                = '{list_podr[rez[i]['РЦ']]['Отв_мастер_тдз']}' WHERE Пномер = {rez[i]['Пномер']}""")



def reload_tbl_rm(ima_table_rm, row_rm, res):
    row_rm_tmp = copy.deepcopy(row_rm)
    print(f'    {ima_table_rm} на месте')
    print(f'     обновление')
    zapros = f'SELECT Пномер_рм FROM {ima_table_rm} WHERE Пномер_рм != "" AND Пномер > 2'
    rez = CSQ.zapros(put_db, zapros, shapka=False)
    rez = [i[0] for i in rez]
    strok_rez = []
    for i in range(len(row_rm_tmp)):
        if row_rm_tmp[i][0] not in rez:
            strok = [row_rm_tmp[i][0], '']
            tek_dat = F.now()
            for i in res:
                if F.strtodate(i, "%Y.%m.%d") <= F.strtodate(tek_dat):
                    strok.append(0)
                else:
                    strok.append(96)
            strok_rez.append(strok)
    CSQ.dob_strok_v_bd_sql(put_db, ima_table_rm, strok_rez)


def reload_jurnal_kplan(dict_podr, res, ima_table_jurnal_kplan,spis_empl):
    def count_empl(podr,spis_empl):
        count = 0
        for i in range(len(spis_empl)):
            if podr in spis_empl[i][4]:
                count+=1
        return count
    print(f'    {ima_table_jurnal_kplan} обновление')

    list_podr_old = CSQ.zapros(db_kplan,f"""SELECT Подразделение FROM {ima_table_jurnal_kplan}""",one_column=True)
    list_to_add = []
    for podr in dict_podr.keys():
        if podr not in list_podr_old:
            list_to_add.append(podr)
    if len(list_to_add) == 0:
        return

    row_empl = []

    for i in range(len(list_to_add)):
        row_empl.append([])
        row_empl[-1].append(list_to_add[i])
        row_empl[-1].append(count_empl(dict_podr[list_to_add[i]]['Наименование'],spis_empl))

    for i in res:
        for podr in row_empl:
            if res[i].value == 1:
                podr.append(0)
            else:
                podr.append(8 * podr[1])
    CSQ.dob_strok_v_bd_sql(db_kplan, ima_table_jurnal_kplan, row_empl)
    print(f'    Добавлены {list_to_add}')

    pass


def check_empl(ima_table_empl,spis_empl,res,conn,dict_rab_vrema):
    if ima_table_empl not in CSQ.spis_tablic(put_db):
        add_tbl_empl(spis_empl, res, ima_table_empl, conn,dict_rab_vrema)
    else:
        print(f'    {ima_table_empl} на месте')
        print(f'     обновление')
        reload_tbl_empl(ima_table_empl, spis_empl, res,dict_rab_vrema)

def check_eq(ima_table_eq, row_equip, res, conn):
    if ima_table_eq not in CSQ.spis_tablic(put_db):
        add_tbl_eq(row_equip, res, ima_table_eq, conn)
    else:
        print(f'    {ima_table_eq} на месте')
        print(f'     обновление')
        reload_tbl_eq(ima_table_eq, row_equip, res)

def check_rm(ima_table_rm, row_rm, res, conn):
    if ima_table_rm not in CSQ.spis_tablic(put_db):
        add_tbl_rm(row_rm, res, ima_table_rm, conn)
    else:
        print(f'    {ima_table_rm} на месте')
        print(f'     обновление')
        reload_tbl_rm(ima_table_rm, row_rm, res)

def check_jurnal_kplan(ima_table_jurnal_kplan, res, spis_empl):
    list_podr = CSQ.zapros(db_kplan, f"""SELECT * FROM podrazdel""", rez_dict=True)
    list_podr = F.raskrit_dict(list_podr, 'Имя')
    if ima_table_jurnal_kplan not in CSQ.spis_tablic(db_kplan):
        add_jurnal_kplan(list_podr, res, ima_table_jurnal_kplan,spis_empl)
    else:
        print(f'    {ima_table_jurnal_kplan} на месте')
        print(f'     обновление')
        reload_jurnal_kplan(list_podr, res, ima_table_jurnal_kplan,spis_empl)

def check_jurnaltdz(ima_table_jurnaltdz, res, conn):
    list_podr = CSQ.zapros(put_db,f"""SELECT * FROM rab_c""", rez_dict=True)
    list_podr = F.raskrit_dict(list_podr,'Код')
    if ima_table_jurnaltdz not in CSQ.spis_tablic(put_db):
        add_tbl_jurnaltdz(list_podr, res, ima_table_jurnaltdz, conn)
    else:
        print(f'    {ima_table_jurnaltdz} на месте')
        print(f'     обновление')
        reload_tbl_jurnaltdz(list_podr, ima_table_jurnaltdz)

def delta_time(nach,konec):
    d_nach = F.shtamp_from_date(f'2022-01-11 {nach}', "%Y-%m-%d %H:%M")
    d_konec = F.shtamp_from_date(f'2022-01-11 {konec}', "%Y-%m-%d %H:%M")
    if d_nach > d_konec:
        d_konec += 86400
    if d_konec - d_nach > 1800:
        delta = (d_konec - d_nach-1800) / 3600
    else:
        delta = (d_konec - d_nach) / 3600
    return round(delta,1)

async def async_func():
    print('==================================')
    print('Проверка производственного календаря...')
    conn, cur = CSQ.connect_bd(put_db)
    spis_empl = CSQ.zapros("","""SELECT * FROM employee""",conn=conn)

    query = f"""SELECT Кол_во, Пномер, "" AS Прим FROM equipment"""
    row_equip = CSQ.zapros(put_db,query,conn=conn,shapka=False)

    query = f"""SELECT Пномер, "" AS Прим FROM rab_mesta"""
    row_rm = CSQ.zapros(put_db,query,conn=conn,shapka=False)

    zapros = """SELECT 
         s1.ФИО as ФИО_1см, s1.Должность as Должность_1см, rab_mesta.Время_начала_1, rab_mesta.Время_конца_1,
         s2.ФИО as ФИО_2см, s2.Должность as Должность_2см, rab_mesta.Время_начала_2, rab_mesta.Время_конца_2,
         s3.ФИО as ФИО_3см, s3.Должность as Должность_3см, rab_mesta.Время_начала_3, rab_mesta.Время_конца_3
         FROM rab_mesta
         INNER JOIN employee s1 ON s1.Пномер == rab_mesta.ФИО_1
         INNER JOIN employee s2 ON s2.Пномер == rab_mesta.ФИО_2
         INNER JOIN employee s3 ON s3.Пномер == rab_mesta.ФИО_3"""
    spis_rab_mesta = CSQ.zapros(put_db, zapros, shapka=True, conn=conn, rez_dict=True)
    dict_rab_vrema = dict()
    for  i in range(len(spis_rab_mesta)):
        dict_rab_vrema[spis_rab_mesta[i]['ФИО_1см']] = delta_time(spis_rab_mesta[i]["Время_начала_1"],spis_rab_mesta[i]["Время_конца_1"])
        dict_rab_vrema[spis_rab_mesta[i]['ФИО_2см']] = delta_time(spis_rab_mesta[i]["Время_начала_2"],
                                                                  spis_rab_mesta[i]["Время_конца_2"])
        dict_rab_vrema[spis_rab_mesta[i]['ФИО_3см']] = delta_time(spis_rab_mesta[i]["Время_начала_3"],
                                                                  spis_rab_mesta[i]["Время_конца_3"])
    for m in months:
        ima_table_empl = 'm_' + str(m).replace('-', '_')
        ima_table_empl_tdz = 'mtdz_' + str(m).replace('-', '_')
        ima_table_eq = 'eq_' + str(m).replace('-', '_')
        ima_table_rm = 'rm_' + str(m).replace('-', '_')
        ima_table_jur_tdz = 'jurnaltdz_' + str(m).replace('-', '_')
        ima_table_jurnal_kplan = 'm_cld_' + str(m).replace('-', '_')
        res = await calendar.month(m)
        check_jurnal_kplan(ima_table_jurnal_kplan, res, spis_empl)
        check_empl(ima_table_empl,spis_empl,res,conn,dict_rab_vrema)
        check_empl(ima_table_empl_tdz, spis_empl, res, conn, dict_rab_vrema)
        check_eq(ima_table_eq, row_equip, res, conn)
        check_rm(ima_table_rm, row_rm, res, conn)
        check_jurnaltdz(ima_table_jur_tdz, res, conn)

    CSQ.close_bd(conn)
    await calendar.close()
    return


loop = asyncio.get_event_loop()
loop.run_until_complete(async_func())
print('==================================')
