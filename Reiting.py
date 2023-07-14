if __name__ != '__main__':
    exit()
try:
    import user_calendar
except:
    pass
import project_cust_38.Cust_Functions as F
import time
import project_cust_38.Cust_virbotka as VIR
import project_cust_38.Cust_SQLite as CSQ
import project_cust_38.Cust_mes as CMS


#pyinstaller.exe --onefile main.py


def update_emploee_to_db(write=False):
    putf = F.tcfg('employee')
    put_db = F.bdcfg('BD_users')
    if F.nalich_file(putf) == False:
        print(f'Не найден файл employee')
        return
    empolee = F.load_file(putf,',')
    spis_empolee = []
    for emploer in empolee:
        if emploer[4] == 'Пауэрз':
            spis_empolee.append([emploer[0],emploer[1],emploer[2],emploer[3],emploer[5],emploer[6]])
    if spis_empolee == []:
        print(f'Файл emploee пустой')
        return
    spis_empolee.append(["","","","","",""])
    spis_empolee.append(["-", "", "", "-", "-", "-"])
    spis_empolee.append(["+", "", "", "+", "+", "+"])
    # |_| вакант = нет
    # - не нужен = нет
    # + не нужен = есть
    conn, cur = CSQ.connect_bd(put_db)
    spis_tabls = CSQ.spis_tablic(put_db,conn = conn)
    if 'employee' not in spis_tabls:
        print('employee таблица не найдена')
        frase = """CREATE TABLE employee (
                    Пномер    INTEGER PRIMARY KEY
                      UNIQUE
                      NOT NULL,
                        ФИО       TEXT    NOT NULL,
                    Должность TEXT    NOT NULL,
                    Статус TEXT     DEFAULT []
                            );"""
        CSQ.sozd_bd_sql(put_db, frase,conn=conn)
        zapros = """INSERT INTO employee
                                      (ФИО, Должность, Статус)
                                      VALUES (?, ?, ?);"""
        CSQ.zapros(put_db, zapros, spisok_spiskov=[['','',''],['-','-',''],['+','+','']], conn=conn)
        print('employee таблица создана')
    print('employee обновление')
    users_db = CSQ.zapros(put_db,'''SELECT * FROM employee WHERE Статус != "Увольнение"''',conn=conn)
    nk_fio = F.nom_kol_po_im_v_shap(users_db,'ФИО')
    nk_dolg = F.nom_kol_po_im_v_shap(users_db, 'Должность')
    nk_stat = F.nom_kol_po_im_v_shap(users_db, 'Статус')
    nk_pnom = F.nom_kol_po_im_v_shap(users_db, 'Пномер')
    nk_podr = F.nom_kol_po_im_v_shap(users_db, 'Подразделение')
    nk_rejim = F.nom_kol_po_im_v_shap(users_db, 'Режим')
    spis_add = []

    #spis_add = []
    #for user in spis_empolee:
    #    fio = ' '.join(user[:3]).strip()
    #    dolg = user[-1]
    #    fl_naid = False
    #    for user_db in users_db:
    #        if fio == user_db[nk_fio] and user_db[nk_stat] == '' and dolg != user_db[nk_dolg]:
    #            fl_naid = True
    #            break
    #    if fl_naid == True:
    #        spis_add = [dolg,fio]
    #        #print(f'Добавлен {fio} {dolg}')
    #        zapros = """UPDATE employee SET
    #                            Должность = ?
    #                              WHERE ФИО = ? and Статус = '';"""
    #
    #        CSQ.zapros(put_db, zapros, spisok_spiskov=spis_add,conn=conn)

    #spis_add = []
    #for user in spis_empolee:
    #    fio = ' '.join(user[:3]).strip()
    #    dolg = user[3]
    #    podr = user[4]
    #    fl_naid = False
    #    for user_db in users_db:
    #        if fio == user_db[nk_fio] and user_db[nk_stat] == '' and podr != user_db[nk_podr]:
    #            fl_naid = True
    #            break
    #    if fl_naid == True:
    #        spis_add = [podr,fio]
    #        #print(f'Добавлен {fio} {dolg}')
    #        zapros = """UPDATE employee SET
    #                            Подразделение = ?
    #                              WHERE ФИО = ? and Статус = '';"""
    #
    #        CSQ.zapros(put_db, zapros, spisok_spiskov=spis_add,conn=conn)


    #spis_add = []
    #for user in spis_empolee:
    #    fio = ' '.join(user[:3]).strip()
    #    print(fio)
    #    dolg = user[3]
    #    podr = user[4]
    #    rejim = user[5]
    #    fl_naid = False
    #    for user_db in users_db:
    #        if fio == user_db[nk_fio] and user_db[nk_stat] == '' and rejim != user_db[nk_rejim]:
    #            fl_naid = True
    #            break
    #    if fl_naid == True:
    #        spis_add = [rejim,fio]
    #        #print(f'Добавлен {fio} {dolg}')
    #        zapros = """UPDATE employee SET
    #                            Режим = ?
    #                              WHERE ФИО = ? and Статус = '';"""
    #
    #        CSQ.zapros(put_db, zapros, spisok_spiskov=spis_add,conn=conn)
    #
    #


    #================= проверка на увольнение(нет фио в списке)
    for i in range(1,len(users_db)):
        fl_naid = False
        if users_db[i][nk_rejim] == 'Абстракт':
            continue
        for user in spis_empolee:
            fio = ' '.join(user[:3]).strip()
            dolg = user[3].replace('.','')
            dolg = dolg.replace('  ', ' ')
            if users_db[i][nk_fio] == fio and users_db[i][nk_dolg] == dolg:
                fl_naid = True
                break
        if fl_naid == False:
            if write:
                CSQ.zapros(put_db,f'''UPDATE employee SET Статус = "Увольнение" WHERE Пномер = {users_db[i][nk_pnom]} ''', conn=conn)
            print(f'{users_db[i][nk_fio]} {users_db[i][nk_dolg]} уволен')
    #==================== проверка на устройство(нет фио в бд)
    for user in spis_empolee:
        fio = ' '.join(user[:3]).strip()
        dolg = user[3].replace('.','')
        dolg = dolg.replace('  ', ' ')
        podr = user[4]
        fl_naid = False
        for user_db in users_db:
            if fio == user_db[nk_fio] and dolg == user_db[nk_dolg] and user_db[nk_stat] == '':
                fl_naid = True
                break
        if fl_naid == False:
            spis_add.append([fio,dolg,'',podr])
            print(f'Добавлен {fio} {dolg} {podr}')
    zapros = """INSERT INTO employee
                              (ФИО, Должность, Статус, Подразделение)
                              VALUES (?, ?, ?, ?);"""
    if write:
        CSQ.zapros(put_db, zapros, spisok_spiskov=spis_add,conn=conn)
    CSQ.close_bd(conn)


vrem = 300
db_users = F.bdcfg('BD_users')
db_naryad = F.bdcfg('Naryad')
db_act = F.bdcfg('BDact')
spis_empolee = F.load_file(F.tcfg('employee'))
DICT_EMPLOEE = CMS.dict_emploee(db_users)


while False:
    try:
        print("Ввести дату для выбора месяца в формате (год-месяц-день) 2020-10-19 или Ввод для текущего месяца")
        vvod = input().strip()
        if vvod == '':
            print(f"Принята текущая дата {F.now()}")
            data = F.now()
            break
        else:
            data = F.strtodate(vvod)
            data = vvod
            break
    except:
        print("Дата не корректная")

data = F.now()
try:
    update_emploee_to_db()
    ans = input('input Y for continue').strip()
    if ans.upper() == "Y" or ans.upper() == "Д":
        print('Обновление')
        update_emploee_to_db(True)
    else: 
        print('Пропуск обновления')
except:
    print(f'ОШИБКА в ФАЙЛЕ emploee')
metka = ''

while True:
    try:
        print(f'#==={F.now()}====#')
        #VIR.perevod_file_s_zp_v_pickle('O:\Производство Powerz\Отдел управления\табель УРВ' + F.sep() + 'employeeFOT.txt',
        #                               F.scfg('BDact') + F.sep() + 'tmp_empl_fant.pickle')
        itog, metka = CMS.raschet_vir(data,db_users,db_naryad,db_act,DICT_EMPLOEE)
        if itog == '':
            print(f'Ошибка {metka}')
            continue
        XML_mssg = []
        XML_mssg.append('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>')
        XML_mssg.append('<Root xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">')
        arr_msg = F.otkr_f("Z:\ProdSoft\Data\Создание\Data\Msg_tel.txt",utf8=True)
        for i in arr_msg:
            XML_mssg.append('        <msg>' + i + "</msg>")
        XML_mssg.append('</Root>')
        F.zap_f("Z:\Data\msg_sbdn.xml",XML_mssg,'',False,True)
        F.zap_f("O:\Журналы и графики\Ведомости для передачи\msg_sbdn.xml", XML_mssg, '', False, True)
        metka = 'формировка таблицы'
        XML = []
        XML.append('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>')
        XML.append('<Root xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">')
        XML.append("<date>" + F.now("%d.%m.%Y %H:%M:%S") + "</date>")
        spis_isp_kol = ['fio','dol','prc','e_prc']
        for i in range(1, len(itog)):
            XML.append('    <Emploe>')
            for j in range(len(itog[i])):
                if itog[0][j] in spis_isp_kol and itog[i][2] < 300:
                    XML.append('        <' + itog[0][j] + '>' + str(itog[i][j])  + "</" + itog[0][j] + ">")
            XML.append('    </Emploe>')
        XML.append('</Root>')
        F.zap_f("Z:\Data\Virabotka_sbdn.xml",XML,'',False,True)
        F.zap_f("Z:\Data\Virabotka_sbdn.txt",itog,'|')
        F.zap_f("O:\Журналы и графики\Ведомости для передачи\Virabotka_sbdn.xml",XML,'',False,True)
        F.zap_f("O:\Журналы и графики\Ведомости для передачи\Virabotka_sbdn.txt",itog,'|')
    except:
        print(f'Ошибка {metka}')
    finally:
        time.sleep(vrem)
#pyinstaller.exe --onefile Reiting.py
