* =======================================================
* Program：CFPS数据清洗及面板数据处理
* Date：2024-07-31
* =======================================================
clear all
clear matrix
set more off

global root         = "/Users/mba/Downloads/Divorce" 
global dofiles      = "$root/Result_data/Dofiles"
global logfiles     = "$root/Result_data/Logfiles"
global temp_data    = "$root/Result_data/Temp_data"
// log using "$logfiles/cfpsclean.log", replace


*- 处理2010年数据


*- 2010年配偶匹配数据
use "$root/CFPS1020/CFPS2010/cfps2010famconf_202008.dta", clear
* 重命名
rename (pid_s tb2_a_p td8_a_p td8_a_s tb3_a_p tb3_a_s tb1b_a_p tb1b_a_s ) (pid_s gender hukou_type hukou_type_s marriage marriage_s age age_s)

// * 删除配偶不在cfps的，因为没法控制变量
// drop if pid_s == -8
// * 只保留男性作为户主
// keep if gender == 1

* 只保留结婚的
keep if marriage == 2
// 57000 obs - 22,781 observations deleted -> 34371 obs
replace marriage_s = 2 if marriage_s == -8
// 135 obs deleted
rename fid fid10

// * 首先确保数据按照家庭分组
// sort fid

// * 创建一个标识符，标记当一个人的pid_s与同一个家庭中的另一个人的pid匹配时
// gen match = 0

// * 循环每个家庭，检查匹配情况
// by fid: gen pid_check = pid
// bysort fid (pid_check): gen partner_matches = 1 if pid_s == pid_check[_n-1] | pid_s == pid_check[_n+1]

// * 保留有匹配的家庭
// bysort fid: egen has_match = max(partner_matches)
// keep if has_match == 1

* 保留个体和配偶pid、fid、配偶相关变量
keep pid pid_s fid10 gender prov hukou_type_s age age_s

save "$temp_data/fam_2010.dta", replace



*- 2010年个人数据
use "$root/CFPS1020/CFPS2010/cfps2010adult_202008.dta", clear
//keep pid fid provcd countyid cid cyear urban income qe1_best qe2 qe201 qe210y qe210m qe211y qe211m qe212 qe213 qe214 qe601 qe602y qe602m qe603y qe603m qe605y_best qe605m qe606y qe606y_best qe606m qe607 qe608 qe609 qe301y qe301m qe302y qe302m qe303 qe310 qe311 qe312 qe313 qe401 qe402y qe402m qe403y_best qe403m qe405y_best qe405m qe406y_best qe406m qe407 qe408 qe409 qe4 

* 把不回答和不知道婚姻次数的都替换了
replace qe210y = qe605y if qe210y == -8
replace qe210m = qe605m if qe210m == -8
replace qe211y = qe606y if qe211y == -8
replace qe211m = qe606m if qe211m == -8

* 重命名婚姻相关变量
ren (qe1_best fid qe210y qe210m qe211y qe211m) (current_status10 fid10 marriage_y marriage_m partner_birth_y partner_birth_m)

* 重命名个人相关变量
ren (qa1y_best qa1m qa102acode qa2 qa201acode qc1 qg2 qg3 qg303 qg305 qg307code qg308code qg309 qg311 qg4 qg401 qg402 qg403 qh1 qh2 qh3 qa302)(birth_y birth_m birth_prov hukou_type hukou degree if_work now_work institution institution_type occupation_type industry_type if_admin working_start_year if_argiculture working_month working_day working_hour agriculture_month agriculture_day agriculture_hour hukou_type_birth)

// * 重命名父亲职业
// ren tb5_code_a_f foccu

* 只保留所需变量
keep pid current_status10 fid10 marriage_y marriage_m partner_birth_y partner_birth_m birth_y birth_m birth_prov hukou_type degree if_work now_work institution institution_type occupation_type industry_type if_admin working_start_year if_argiculture working_month working_day working_hour agriculture_month agriculture_day agriculture_hour gender provcd hukou hukou_type_birth fparty feduc foccupcode meduc mparty moccupcode
 

* 只保留2010年以前结婚且现在仍在婚的个体
keep if current_status10 == 2

* 这些个体的if_orginal默认为1
gen if_original = 1

save "$temp_data/adult_2010_temp.dta", replace

* adult_2010_temp.dta与fam_2010.dta的pid合并
use "$temp_data/fam_2010.dta", clear
merge 1:1 pid using "$temp_data/adult_2010_temp.dta", force
drop if  pid_s == . 
drop _merge
save "$temp_data/adult_2010_temp_merge.dta", replace

* adult_2010_temp.dta与fam_2010.dta的pid_s合并
use "$temp_data/adult_2010_temp.dta", clear
ren * *_s
drop if pid_s==.
save "$temp_data/adult_2010_temp_s.dta", replace

use "$temp_data/adult_2010_temp_merge.dta", clear
drop if pid_s == .
drop if pid_s == -8 
merge 1:1 pid_s using "$temp_data/adult_2010_temp_s.dta", force
drop if pid==.
drop _merge


* 储存数据
save "$temp_data/adult_2010.dta", replace



// * 生成2010年已婚pid与结婚时间数据
// use "$temp_data/adult_2010.dta", replace
// keep pid marriage_y marriage_m
// save "$temp_data/2010_pid_marriage_y.dta", replace


*- 2010年家庭收入及房产数据
use "$root/CFPS1020/CFPS2010/cfps2010famecon_202008.dta", clear

* 房屋相关变量重命名
ren (fid fd1 fd2 fd3 fd4 fd6 fd101_s_1 fd101_s_2 fd101_s_3) (fid10 chanquan area time value type owner_1 owner_2 owner_3 )

* 收入相关变量重命名
ren (faminc indinc) (income income_per)


/*
fd1 现在居住房子产权 1. 完全自有【跳至 D101】 2. 和单位共有产权【跳至D110】 3. 租住【跳至D120】 4. 政府免费提供【跳至D2】 5. 单位免费提供【跳至D2】 6. 父母/子女提供【跳至D2】 7. 其他亲友借住【跳至D2】

fd101_s_1 fd101_s_2 fd101_s_3 完全自有房登记的所有者
fd105 完全自有房购买时间

fd110 共有产权登记所有者
fd111 共有产权购买时间

fd2 居住房的建筑面积 1..1000 平方米

fd4 上个月，您家现居住房子的市值 0.0..99999.0 万元

fd6 现居住房屋的类型 1. 单元房 2. 平房 3. 四合院 4. 别墅 5. 联排别墅 6. 小楼房 77. 其他【请注明】______ FD6SP"房屋类型注明"


*/


* 生成购房时间（不区分自有产权和共有产权）
gen house_time = fd105
replace house_time = fd111 if chanquan == 2
label var house_time  "哪年购买的"

* 将自有产权和共有产权的第一所有者合并
replace owner_1 = fd110 if chanquan == 2

* 去掉中间变量
keep fid10 chanquan area time value type owner_1 owner_2 owner_3 income income_per

// obs 14797
save "$temp_data/house_2010.dta", replace

* 婚姻和房产数据集以fid合并
use "$temp_data/adult_2010.dta", clear
merge m:1 fid10 using "$temp_data/house_2010.dta" 

* 只保留合并成功的项目
drop _merge


ren * *10
ren *1010 *10
ren pid10 pid
drop if pid==.
* 储存数据
save "$temp_data/data_2010.dta", replace


*- 处理2012年数据

*- 2012年配偶匹配数据
use "$root/CFPS1020/CFPS2012/cfps2012famconf_092015.dta", clear
* 重命名
rename (tb2_a_p tb3_a12_p tb3_a12_s) (gender marriage marriage_s )
* 只保留结婚或离婚的，去掉丧偶或单身
keep if marriage == 2 | marriage == 4
// 57000 obs - 22,781 observations deleted -> 34371 obs

keep pid fid10 fid12 pid_s gender marriage

sort pid
* 创建一个变量，计算每个 pid 出现的次数
by pid: gen pid_count = _N
// 保留以下记录：
// pid 只出现一次的个体（pid_count == 1）
// pid 重复出现的个体中，fid12 不等于 fid10 的记录
keep if pid_count == 1 | (pid_count > 1 & fid12 != fid10) 


save "$temp_data/fam_2012.dta", replace

*- 2012年婚姻数据
use "$root/CFPS1020/CFPS2012/cfps2012adult_201906.dta", clear


// ren (qe104 qe201 qe202 qe203y qe203m qe208y qe208m) (current_status if_original original_reason orginal_divorce_y orginal_divorce_m new_marriage_y new_marriage_m)

* 婚姻重命名
ren (qe104 qe201 qe202 qe203y qe203m qec104 qe209a qec105y qec105m ) (current_status12 if_original original_reason orginal_divorce_y orginal_divorce_m marriage_date_check degree_s original_marriage_y original_marriage_m )

* 户口
replace qa302acode = provcd if qa302 !=5
ren (qa301 qa302ccode qa503 )(hukou_type hukou hukou_type_birth)

* 个体数据
ren (cfps2012_gender_best qv201b qv103code_best qv104 qv102 qv203code_best qv204 qv202) (gender age foccupcode fparty feduc moccupcode mparty meduc)

/*

2012年问卷婚姻状况跳转规则：
提取2010年婚姻状况数据，记录在cfps2010_marriage。此时有一部分人显示1-5，有一部分人显示没有数据（0），因为2010年并没有进入调查。
	若2010年访问过，则qe101询问初访婚姻状况 - 如果qe101!= cfps2010_marriage，则再次询问，记录在qe102
	若2010年没有访问过，则qe103询问当时婚姻情况
qe104询问现在婚姻状况

首先使用cfps2010_marriage，提取2010年婚姻状态。

如果2010年婚姻状态不为空，则确认qe101 == cfps2010_marriage，如果qe101!= cfps2010_marriage，则用qe102替换。如果相等，直接使用这个值。

如果2010年婚姻状态为空，则直接替换qe104
*/

* 生成2010年婚姻状态
gen marriage10 = cfps2010_marriage if qe101 == cfps2010_marriage
replace marriage10 = qe102 if qe101 != cfps2010_marriage
replace marriage10 = qe103 if cfps2010_marriage<0

* 只保留在婚和离婚的个体
keep if current_status12 ==2 | current_status12 == 4
* 去掉2010年单身的
drop if cfps2010_marriage ==1 | cfps2010_marriage==3 | cfps2010_marriage==4 | cfps2010_marriage==-8
* 去掉丧偶
keep if original_reason != 5

/*
2012年问卷在婚跳转规则：
e104=2 在婚部分

cfps2010_marriage=2: qe201 是否初访配偶，只保留1

cfps2010_marriage=0: 跳至qe208 与现配偶结婚时间 qe209学历 

*/

* 对于2010结婚且2012继续结婚的，只保留是初访配偶的个体
replace original_marriage_y = qe208y if cfps2010_marriage==0 & current_status12==2


/*
2012年问卷离婚跳转规则：
如果cfps2010_marriage=2（已婚）且E104=4（离婚），跳至qe402：离婚对象是否是上次的配偶，如果qe402=1（是），则跳至qe412：与上一任配偶离婚的时间，则跳至婚姻确认模块（EC3 离婚配偶确认）。
qec301 核对与上任离婚时间，qec301=1（正确）不处理，qe301=0（错误）则回答qec302。qec303核对和上一任结婚时间，qec303=1（正确）不处理，qec303=0（错误）则回答qec304

如果cfps2010_marriage=0（2010年没有调查），则跳至E412，qe412：与上一任配偶离婚的时间，qe413：与上一任配偶结婚的时间。跳至婚姻确认模块。qec302记录和上一任离婚的时间，qec304记录和上一任结婚的时间。
*/


keep pid fid10 fid12 provcd gender age current_status12 if_original original_reason orginal_divorce_y orginal_divorce_m marriage_date_check degree_s original_marriage_y original_marriage_m hukou_type hukou hukou_type_birth foccupcode fparty feduc moccupcode mparty meduc marriage10

sort pid
* 创建一个变量，计算每个 pid 出现的次数
by pid: gen pid_count = _N
// 保留以下记录：
// pid 只出现一次的个体（pid_count == 1）
// pid 重复出现的个体中，fid12 不等于 fid10 的记录
keep if pid_count == 1 | (pid_count > 1 & fid12 != fid10) 
drop pid_count

save "$temp_data/adult_2012_temp.dta", replace


use "$temp_data/fam_2012.dta", clear
drop if pid==-8
merge 1:1 pid using "$temp_data/adult_2012_temp.dta", force
//     Result                           # of obs.
//     -----------------------------------------
//     not matched                         4,930
//         from master                     4,284  (_merge==1) -------- fam数据有，但没接受个体调查
//         from using                        646  (_merge==2) -------- 接受了个体调查但是没有配偶信息

//     matched                            26,658  (_merge==3)
//     -----------------------------------------

drop _merge
save "$temp_data/adult_2012_temp_merge.dta", replace

* adult_2010_temp.dta与fam_2010.dta的pid_s合并
use "$temp_data/adult_2012_temp.dta",clear
ren * *_s
ren fid12_s fid12
// drop if pid_s==.
save "$temp_data/adult_2012_temp_s.dta", replace

use "$temp_data/adult_2012_temp_merge.dta", clear
// drop if pid_s == .
// drop if pid_s == -8 drop if pid_s == -8 &
merge m:1 pid_s using "$temp_data/adult_2012_temp_s.dta", force
//     Result                           # of obs.
//     -----------------------------------------
//     not matched                         6,012
//         from master                     5,146  (_merge==1)
//         from using                        866  (_merge==2)

//     matched                            26,442  (_merge==3)
//     -----------------------------------------
drop _merge pid_count
drop if pid==.

sort pid
* 创建一个变量，计算每个 pid 出现的次数
by pid: gen pid_count = _N
// 保留以下记录：
// pid 只出现一次的个体（pid_count == 1）
// pid 重复出现的个体中，fid12 不等于 fid10 的记录
keep if pid_count == 1 | (pid_count > 1 & fid12 != fid10) 
drop pid_count

ren * *12
ren *1212 *12
ren pid12 pid
ren fid1012 fid10

save "$temp_data/adult_2012.dta", replace





use "$temp_data/adult_2012.dta", clear
* 与2010年已婚pid与结婚时间数据合并

merge 1:1 pid using "$temp_data/data_2010.dta", force
/*

    Result                           # of obs.
    -----------------------------------------
    not matched                        11,998
        from master                     6,691  (_merge==1)
        from using                      5,307  (_merge==2)

    matched                            21,344  (_merge==3)
    -----------------------------------------




master only: 只出现在2012年数据中，个体没有参与2010年调查。但可以通过marriage10判断2010年结婚情况。
using only：只出现在2010年数据中，个体没参与2012年调查

如果是master only，且结婚年龄不知道，则删除，因为不知道是否2010年以前结婚的

*/




* 如果marriage_y（2010年调研的结婚时间）值不为空，且if_original = 1（仍为2010年时的伴侣），则使用marriage_y
* 如果marriage_y（2010年调研结婚时间）值为空，证明2010年没有接受调研，


* 如果是using only，证明只有2010年接受了访谈，2012年没有接受访谈
* 只出现在2010数据中：2010结婚状态为已婚


* 如果是master only，只出现在2012年数据中
* 如果结婚年份不知道，则删除，因为不知道是否2010年以前结婚的
drop if _merge == 1 & marriage_y10 == .

* 如果2012年在婚，且结婚时间早于2010年，且为原来的配偶，则2010年标记为在婚
replace current_status10 = 2 if current_status12 == 2 & original_marriage_y12<2010 & if_original12==1

* 如果marriage_date_check=0（错误），则将marriage_y替换为original_marriage_y
replace marriage_y10 = original_marriage_y12 if marriage_date_check12==5 | marriage_date_check12==-8
replace marriage_m10 = original_marriage_m12 if marriage_date_check12==5 | marriage_date_check12==-8

drop _merge marriage_date_check12
* 标记不知道结婚年份的
// gen dont_know = 1 if marriage_y12 == -1
// label var dont_know "不知道结婚年份"
// gen not_applicable = 1 if marriage_y12 == -8 | marriage_y12 == -2
// label var not_applicable "结婚年份不适用"


// * 只保留所需列
// keep pid cyear fid10 fid12 current_status if_original orginal_divorce_y orginal_divorce_m marriage_y marriage_m marriage10

* 储存数据
save "$temp_data/adult_2012.dta", replace


*- 2012年房产收入数据
use "$root/CFPS1020/CFPS2012/cfps2012famecon_201906.dta", clear

/*
fq1
Q1 FQ1”您家现住房归谁所有”您家现在住的房子归谁所有? 1.家庭成员拥有完全产权(跳至 Q2) 2.家庭成员拥有部分产权(继续回答 Q101) 3.公房(单位提供的房子)(继续回答 Q101) 4.廉租房(继续回答 Q101)
5.公租房(继续回答 Q101)
6.市场上租的商品房(跳至 Q102)
7.亲戚、朋友的房子(继续回答 Q101)
77. FQ1SP”其他归属，请注明”其他【请注明】______(继续回答 Q101)

fq701
Q701 FQ701”现住房的面积(m2)”您家现在住房的建筑面积是多大? _____1..10,000 平方米。 F1:“建筑面积”是指供人居住使用的房屋建筑面积。住宅建筑面积测算方式因楼层、结构等 的不同而不同，以房产证、租房合同为准。自建房屋可做简单的丈量，住宅建筑面积=占地 的长×宽×楼层数，计量单位为平方米。

fq4a_best
Q4 您家现在居住的这所房子当前的市场总价是多少?____FQ4A”房子当前市价(万)”万 ____FQ4B”房子当前市价(千)”千____FQ4C”房子当前市价(百)”百____FQ4D”房子当前 市价(十)”十____FQ4E”房子当前市价(元)”元
【data】计算生成房屋价格 houseprice1。
【CAPI】
#1 如果 houseprice1>=1,000,000 或<=1,000，继续回答 Q4CKP;否则跳至 Q5。 #2 如果 Q4 选择“不知道”或“拒答”继续回答 Q401;否则跳至 Q5。

fq3_s_1 fq3_s_2 fq3_s_3 fq3_s_4 fq3_s_5 fq3_s_6 fq3_s_7 fq3_s_8 房产证上的名字

fq6 Q6 FQ6”何时获住房产权”您家什么时候获得现在住的这所房子的产权?_________ 1900..2012 年
访员注意:用 4 位数表示年。
*/
ren (fq1 fq4a_best fq701_best fb5 fq6 fq3_s_1 fq3_s_2 fq3_s_3 fq3_s_4 fq3_s_5 fq3_s_6 fq3_s_7 fq3_s_8) (chanquan value area type house_time owner_1 owner_2 owner_3 owner_4 owner_5 owner_6 owner_7 owner_8)

ren(fincome1_adj fincome1_per_adj) (income income_per)

keep fid10 fid12 chanquan value area type house_time owner_1 owner_2 owner_3 owner_4 owner_5 owner_6 owner_7 owner_8 income income_per 

ren * *12
ren(fid1212 fid1012)(fid12 fid10)
save "$temp_data/house_2012.dta", replace

* 婚姻和房产数据集合并
use "$temp_data/adult_2012.dta", clear
merge m:1 fid12 using "$temp_data/house_2012.dta" 

* 只保留合并成功的项目
// keep if _merge==3
drop _merge 
//ren marriage marriage12

* 储存数据
save "$temp_data/data_2012_2010.dta", replace











*- 处理2014年数据

*- 2014年配偶匹配数据
use "$root/CFPS1020/CFPS2014/cfps2014famconf_170630.dta", clear
* 重命名
rename (pid_s tb2_a_p tb3_a14_p tb3_a14_s) (pid_s gender marriage14 marriage14_s)
keep if marriage14 == 2 | marriage14 == 4
sort pid
* 创建一个变量，计算每个 pid 出现的次数
by pid: gen pid_count = _N
// 保留以下记录：
// pid 只出现一次的个体（pid_count == 1）
// pid 重复出现的个体中，fid12 不等于 fid10 的记录
keep if pid_count == 1 | (pid_count > 1 & fid14 != fid12) 

keep pid pid_s fid10 fid12 fid14 marriage14 marriage14_s 


gen pid_str = string(pid, "%12.0g")
gen pid_first6 = substr(pid_str, 1, 6)
gen pid_first6_num = real(pid_first6)

duplicates tag pid, generate(dup_tag)
drop if dup_tag > 0 & pid_first6_num != fid14

save "$temp_data/fam_2014.dta", replace

*- 2014年婚姻数据
use "$root/CFPS1020/CFPS2014/cfps2014adult_201906.dta", clear


* 重命名婚姻相关变量
ren (qea0 eeb301 cfps2012_marriage qea202 qea203code) (current_status14 original_reason marriage12 degree_s occupation_s)

* 重命名户口变量
replace qa302ccode = provcd if qa302ccode == -8
ren (qa302ccode qa301 qa503)(hukou hukou_type hukou_type_birth)

// * 重命名收入相关变量
// ren (qg1202)(income_y)
/*
2014年问卷跳转规则：
询问当前婚姻状态qea0

提取2012年婚姻状况数据，记录在cfps2012_marriage。此时有一部分人显示1-5，有一部分人显示没有数据（0），因为2012年并没有进入调查。
	若2012年访问过，则qea1询问初访婚姻状况
		如果qea1 = 1（是），则将cfps2012_marriage作为2012年婚姻状况
		如果qea1 = 0（否），则再次询问，记录在qea2，以qea2作为2012年婚姻状况
	若2010年没有访问过，则qea2询问当时婚姻情况，以qea2作为2012年婚姻状况
*/

* 生成2012年婚姻状态
// gen marriage12 = cfps2012_marriage if qea1 == 1
// replace marriage12 = qea2 if qea1 == 0


/*
. table marriage12 

----------------------
marriage_ |
2012      |      Freq.
----------+-----------
        1 |      3,434
        2 |     22,047
        3 |        105
        4 |        349
        5 |      1,353
----------------------
*/



* 保留2012年结婚的个体
keep if marriage12==2

* 保留2014年结婚与离婚状态的个体
* 去掉离婚中的丧偶的个体
keep if current_status14 ==2 | current_status14 == 4
keep if original_reason != 2

/*
2014年在婚和离婚跳转规则：
cfps2012_marriage=2，或cfps2012_marriage=0，qea2询问2012年婚姻状态，qea2=2
	ea205结婚日期，eb202y婚姻持续年份，eb202m婚姻持续月份，eb202c是否当前
		eb202c=1，则持续到当前，那eb202y和eb202m无意义，婚姻提问结束
		eb202c=0，未持续到当前，那eb202y和eb202m成为离婚时间，eb301 （已重命名为original_reason）分开原因 eb4接下来是否结婚
		
现在在婚，且eeb2是否与同居伙伴结婚=1，则提问eeb201y结婚年份，eeb201m结婚月份

cfps2012_marriage=4：离婚年份qea208y 离婚月份qea208m
*/

gen orginal_divorce_y = eeb202y if eeb202c==0
gen orginal_divorce_m = eeb202m if eeb202c==0

gen marriage_y = qea205y if qea205y != -8
gen marriage_m = qea205m if qea205m != -8


// * 去掉接下来结婚的（虽然这个数据集里没有）
// drop if eeb409_a_1 == 1


// * 去掉是否原本伴侣回答不适用的
// drop if if_original < 0 
// drop if if_original ==.

* 标记不知道结婚年份的
gen dont_know = 1 if marriage_y == -1
label var dont_know "不知道结婚年份"
gen not_applicable = 1 if marriage_y == -8 | marriage_y == -2
label var not_applicable "结婚年份不适用"

ren (eeb202c cfps2014_age cfps_gender) (if_original age gender)

* 只保留所需列
keep pid provcd cyear fid10 fid12 fid14 current_status14 marriage12 if_original degree_s occupation_s orginal_divorce_y orginal_divorce_m marriage_y marriage_m dont_know not_applicable age gender original_reason marriage12 degree_s occupation_s hukou hukou_type hukou_type_birth

//duplicates report pid
//无重复

save "$temp_data/adult_2014_temp.dta", replace

*- adult_2014与fam_2014合并
use "$temp_data/fam_2014.dta", clear
merge 1:1 pid using "$temp_data/adult_2014_temp.dta", force
drop _merge
save "$temp_data/adult_2014_temp_merge.dta", replace

use "$temp_data/adult_2014_temp.dta",clear
ren * *_s
save "$temp_data/adult_2014_temp_s.dta", replace

use "$temp_data/adult_2014_temp_merge.dta", clear
merge m:1 pid_s using "$temp_data/adult_2014_temp_s.dta", force

drop pid_str pid_first6 pid_first6_num dup_tag

save "$temp_data/adult_2014.dta", replace


*- 2014年房产数据
use "$root/CFPS1020/CFPS2014/cfps2014famecon_201906.dta", clear

/*
fq2 Q2 FQ2”您家现住房归谁所有”您家现在住的房子归谁所有？现在居住房子产权
1.家庭成员拥有完全产权
2.家庭成员拥有部分产权
3.公房（单位提供的房子）
4.廉租房
5.公租房
6.市场上租的商品房
7.亲戚、朋友的房子
77. FQ2SP”其他归属，请注明”其他【请注明】
______
F1：(1)“廉租房”是指政府以租金补贴或实物配租的方式，向符合城镇居民最低生活保障
标准且住房困难的家庭提供社会保障性质的住房。廉租房的分配形式以租金补贴为
主，实物配租和租金减免为辅。
(2)“公租房”是“公共租赁房”的简称，指的是将政府或公共机构所有的房屋，以低
于市场价或者承租者承受得起的价格租给住房困难群体。

fq801 Q801 FQ801”现住房面积（平方米）”您家现住房的建筑面积是多少平方米？
_____
1..10,000
平方米。
F1：
“建筑面积”是指供人居住使用的房屋建筑面积。住宅建筑面积测算方式因楼层、结构
等的不同而不同，以房产证、租房合同为准。自建房屋可做简单的丈量，住宅建筑面积＝占
地的长× 宽× 楼层数，计量单位为平方米。


fq6 Q6 FQ6”房子当前市价（万元）”您估计您家现在居住的这所房子当前的市场总价是多少万
元？
____
0.01..10,000 万

fd6 现居住房屋的类型 1. 单元房 2. 平房 3. 四合院 4. 别墅 5. 联排别墅 6. 小楼房 77. 其他【请注明】______ FD6SP"房屋类型注明"

fhz1 HZ1FHZ1"居住房屋类型"受访家户居住房屋的类型是什么？
1. 单元房 2. 平房 3. 四合院 4. 别墅 5. 联排别墅
6. 小楼房 77. 其他【请注明】

FQ4 房屋购买建造年份（年）”您家现在居住的这所房子是哪一年买的或建的？
______
1910…2014 年


Q8 FQ8”住房面积是否变化”从“ 【CAPI】加载上一次调查年月”至今，您家现住房的住房
面积有没有变化？
1. 没变化（跳至 R 部分） 3. 变大 5.变小
【CAPI】Q8=“不知道”或拒绝回答跳至 R 部分。
*/


* 房产变量
rename (fq2 fq801 fq6 fhz1 fq4 fq3pid_a_1 fq3pid_a_2 fq3pid_a_3 fq3pid_a_4 fq3pid_a_5 fq3pid_a_6 fq3pid_a_7 fq3pid_a_8 fq8 fincome1 fincome1_per)  (chanquan area value type house_time owner_1 owner_2 owner_3 owner_4 owner_5 owner_6 owner_7 owner_8 change income income_per)


keep fid14 chanquan area value type house_time owner_1 owner_2 owner_3 owner_4 owner_5 owner_6 owner_7 owner_8 change income income_per
save "$temp_data/house_2014.dta", replace

* 婚姻和房产数据集以fid合并
use "$temp_data/adult_2014.dta", clear
drop _merge
merge m:1 fid14 using "$temp_data/house_2014.dta" 

* 只保留合并成功的项目
keep if _merge==3
drop _merge
ren *_s *14_s
ren * *14
ren *14_s14 *_s14
ren pid14 pid

* 储存数据
save "$temp_data/data_2014.dta", replace

*- 2014年和2010、2012年数据合并
use "$temp_data/data_2012_2010.dta", clear
merge m:1 pid using "$temp_data/data_2014.dta"

/*
    Result                           # of obs.
    -----------------------------------------
    not matched                        21,276
        from master                    21,247  (_merge==1)
        from using                         29  (_merge==2)

    matched                                53  (_merge==3)
    -----------------------------------------


master only: 只出现在2014年数据中，个体没有参与2012年调查。但可以通过marriage12判断2012年结婚情况。
using only：只出现在2012年数据中，个体没参与2014年调查

如果是master only，且结婚年份不知道，则删除，因为不知道是否2010年以前结婚的
*/


* 如果是using only，证明只有2012年接受了访谈，2014年没有接受访谈
replace marriage12 = current_status12 if marriage12 == .
ren marriage1012 marriage10
replace marriage10 = current_status10 if marriage10==-8 | marriage10 == .

drop _merge
save "$temp_data/data_2014_2010.dta", replace





*- 处理2016年数据
*- 2016年配偶数据

use "$root/CFPS1020/CFPS2016/cfps2016famconf_201804.dta", clear
rename (pid_s tb2_a_p tb3_a16_p tb3_a16_s) (pid_s gender marriage marriage_s)

keep if marriage == 2 | marriage == 4
// keep pid fid16 fid14 fid12 fid10 provcd countyid cid cyear  urban16 qea1 qea2 qea0 qea201y qea201m qea202 qea203code qea204 qea205y qea205m qea206 qea207 qea2071 qea208y qea208m qea209y qea209m qea210 qea211y qea211m qea2111 eeb202y eeb202m eeb202_1 eeb301

/*
qea1 qea2 2014年婚姻状况确认
qea0 当前婚姻状态
2014年配偶离婚/过世：
qea201y 配偶出生年份
qea201m 配偶出生月份
qea202 qea203code qea204 
qea205y 配偶结婚年份
qea205m 配偶结婚月份
qea206 qea207 qea2071 qea208y 
qea208m 配偶离婚年份
qea209y 配偶离婚月份
qea209m qea210 qea211y qea211m qea2111


*/
keep pid pid_s fid16 fid14 fid12 fid10 gender marriage marriage_s
save "$temp_data/fam_2016.dta", replace

*- 2016年个人数据

use "$root/CFPS1020/CFPS2016/cfps2016adult_201906.dta", clear


* 重命名
rename (qea0 qea1 eeb202y eeb202m eeb202_1 eeb301 qea205y qea205m)(current_status16 marriage_check orginal_divorce_y orginal_divorce_m if_original original_reason marriage_y marriage_m)

replace pa302ccode = provcd if pa302ccode == -8
ren (pa503 pa301 pa302ccode)(hukou_birth_type hukou_type hukou )
// qea1 是确认，不用)
// 1.未婚 2.有配偶（在婚） 3.同居 4.离婚 5.丧偶

* 只保留2014年结婚、2016年在婚和离婚的个体，且非丧偶
keep if current_status16 ==2 | current_status16 == 4
keep if original_reason != 2

/*
加载2014年调查结果cfps2014_marriage
	cfps2014_marriage!=-8：2014年调查了
		qea1 调查婚姻状况确认
		qea2 生成cfps2014_marriage



	cfps2014_marriage=-8：2014年未调查
		ea2 您“ 【CAPI】加载 CFPS2014_time 调查时/2014 年 1 月 1 日”时的婚姻状况是
		
询问2014年时的配偶

	qea201 出生时间
	qea202 学历
	qea203 职业
	
	
	marriage14=2: 
		qea205 结婚时间
		eb406_1 是否持续到当前
		
	marriage14=4:
		qea205 结婚时间
		qea208 离婚时间



*/

* 更新2014年婚姻状况
gen marriage14 = qea2 if cfps2014_marriage!=-8 & marriage_check == 0
replace marriage14 = cfps2014_marriage if marriage_check == 1

// * 去掉2010年之后又结婚的
// drop if  eeb201y > 2010



* 只保留所需列
keep pid cyear fid10 fid12 fid14 fid16 provcd current_status16 orginal_divorce_y orginal_divorce_m if_original original_reason marriage_y marriage_m marriage14 hukou_birth_type hukou_type hukou


save "$temp_data/adult_2016.dta", replace



* adult_2016_temp.dta与fam_2016.dta的pid合并

use "$temp_data/fam_2016.dta"
merge 1:1 pid using "$temp_data/adult_2016.dta"
drop _merge
save "$temp_data/adult_2016_temp_merge.dta", replace

use "$temp_data/adult_2016.dta",clear
ren * *_s
//ren (pid16_s fid1216_s fid1016_s fid1416_s fid1616_s) (pid_s fid12 fid10 fid14 fid16)
save "$temp_data/adult_2016_temp_s.dta", replace


use "$temp_data/adult_2016_temp_merge.dta", clear
merge m:1 pid_s using "$temp_data/adult_2016_temp_s.dta", force
drop _merge
save "$temp_data/adult_2016.dta", replace



*- 2016年收入和房产数据
use "$root/CFPS1020/CFPS2016/cfps2016famecon_201807.dta", clear
/*

Q2 FQ2“您家现住房归谁所有”您家现在住的房子归谁所有？
1. 家庭成员拥有完全产权
2. 家庭成员拥有部分产权
3. 公房（单位提供的房子）
4. 廉租房
5. 公租房
6. 市场上租的商品房
7. 亲戚、朋友的房子
77.其他【请注明】 FQ2SP“其他归属，请注明”


Q801 FQ801“现住房面积（平方米） ”您家现住房的建筑面积是多少平方米？ 1.0 ..
10,000.0 平方米。
F1：“建筑面积”是指供人居住使用的房屋建筑面积。住宅建筑面积测算方式因楼层、结构等
的不同而不同，以房产证、租房合同为准。自建房屋可做简单的丈量，住宅建筑面积＝ 占地的
长× 宽× 楼层数，计量单位为平方米。

Q6 FQ6
元？ 0.01..10,000 万
您估计您家现在居住的这所房子当前的市场总价是多少万
F1：房屋当前市场总价指的是房屋及其所在的宅基地（仅针对农村）转让所能够获得的收
益。

HZ1FHZ1"居住房屋类型"受访家户居住房屋的类型是什么？
1. 单元房 2. 平房 3. 四合院 4. 别墅 5. 联排别墅
6. 小楼房 77. 其他【请注明】

Q8 FQ8“住房面积是否变化”从“ 【CAPI】CFPS_lastintervtime”至今，您家现住房的住房
面积有没有发生改变？
1. 没变化 3. 变大 5.变小
【CAPI】Q8=3 或 5，继续回答Q801；否则，跳至 R 部分。
*/


*- 重命名房产变量
rename (fq2 fq801 fq6 fhz1 fq4 fq3pid_a_1 fq3pid_a_2 fq3pid_a_3 fq3pid_a_4 fq3pid_a_5 fq3pid_a_6 fq3pid_a_7 fq3pid_a_8 fq3pid_a_9 fq3pid_a_10 fq8) (chanquan area value type house_time owner_1 owner_2 owner_3 owner_4 owner_5 owner_6 owner_7 owner_8 owner_9 owner_10 change)

*- 重命名收入变量
ren (fincome1 fincome1_per)(income income_per)
keep chanquan area value type house_time owner_1 owner_2 owner_3 owner_4 owner_5 owner_6 owner_7 owner_8 owner_9 owner_10 change income income_per fid16
save "$temp_data/house_2016.dta", replace

*- 婚姻和房产数据集以fid合并
use "$temp_data/adult_2016.dta", clear
merge m:1 fid16 using "$temp_data/house_2016.dta", force

*- 只保留合并成功的项目
keep if _merge==3
drop _merge
drop if pid==.
ren * *16
ren (pid16 fid1616 provcd1616) (pid fid16 provcd16)

*- 储存数据
save "$temp_data/data_2016.dta", replace




*- 合并数据

use "$temp_data/data_2014_2010.dta", clear
drop if pid==. | pid==-8

merge 1:1 pid using "$temp_data/data_2016.dta", force

/*

    Result                           # of obs.
    -----------------------------------------
    not matched                        11,928
        from master                     4,130  (_merge==1)
        from using                      7,798  (_merge==2)

    matched                            20,892  (_merge==3)
    -----------------------------------------


*/



//replace current_status1616 = marriage1414 if current_status1616 == .
 
// drop if_original original_reason marriage14 _merge

//ren current_status14_s14 marriage14_s
drop _merge

 
save "$temp_data/data_2016_2010.dta", replace




*- 处理2018年数据
*- 2018年配偶数据
use "$root/CFPS1020/cfps2018/cfps2018famconf_202008.dta", clear
ren (pid_a_s tb2_a_p tb3_a18_p tb3_a18_s ) (pid_s gender marriage marriage_s)
keep if marriage == 2 | marriage==4
keep pid fid18 fid10 fid12 fid14 fid16 pid_s gender marriage marriage_s
save "$temp_data/fam_2018.dta", replace

*- 2018年婚姻数据
use "$root/CFPS1020/CFPS2018/cfps2018person_202012.dta", clear

// keep fid18 fid16 fid14 fid12 fid10 pid provcd countyid cid cyear  urban18 qea0 qea1 qea2 qea201y qea201m qea202 qea203code qea204 qea205y qea205m qea206 qea207 qea2071 qea208y qea208m qea209y qea209m qea210code qea211y qea211m qea2111 qeb405_1_a_1 qeb405_1_a_2 qea0 eeb202y eeb202m eb202_1 eeb301

/*
qea0 current_status
//1.未婚 2.有配偶（在婚） 3.同居 4.离婚 5.丧偶
*/

/*
EA 上期婚姻状况
qea0 当前婚姻状况
qea1 上期婚姻状况确认
qea2 如果不对或marriage_last==-8则重新提问上期婚姻状况

ea201 出生时间
ea202 学历
ea203 职业

marriage_last_update=2、4 或 5：
ea205 结婚时间

ea208 离婚时间应
跳至EB
eeb1=1 还结婚过 


*/

* 重命名
rename (qea0 eeb202y eeb202m eb202_1 eeb301 qea205y qea205m)(current_status18 orginal_divorce_y orginal_divorce_m if_original original_reason marriage_y marriage_m)
// qea1 是确认，不用)
// 1.未婚 2.有配偶（在婚） 3.同居 4.离婚 5.丧偶

* 户口
replace qa302a_code = provcd if qa302 != 6
ren (qa301 qa302a_code qa603)(hukou_type hukou hukou_birth_type)
* 2016年婚姻状况
gen marriage16 = qea2 if qea1 == 0
replace marriage16 = marriage_last_update if qea1==1

* 只保留在婚和离婚的个体，且非丧偶
keep if current_status18 ==2 | current_status18 == 4
keep if original_reason != 2

* 只保留所需列
keep pid cyear fid10 fid12 fid14 fid16 fid18 current_status18 marriage16 orginal_divorce_y orginal_divorce_m if_original original_reason marriage_y marriage_m hukou_type hukou hukou_birth_type
save "$temp_data/adult_2018_temp.dta", replace

* adult_2018_temp.dta和fam_2018.dta合并
use "$temp_data/adult_2018_temp.dta",clear
ren * *_s
ren (pid_s fid18_s fid16_s fid14_s fid12_s fid10_s) (pid_s fid18 fid16 fid14 fid12 fid10)
save "$temp_data/adult_2018_temp_s.dta", replace

use "$temp_data/fam_2018.dta"
merge 1:1 pid using "$temp_data/adult_2018_temp.dta", force
drop _merge
merge m:1 pid_s using "$temp_data/adult_2018_temp_s.dta", force
save "$temp_data/adult_2018.dta", replace


*- 2018年房产及收入数据
use "$root/CFPS1020/CFPS2018/cfps2018famecon_202101.dta", clear


/*

Q2 FQ2“您家现住房归谁所有”您家现在住的房子归谁所有？
1. 家庭成员拥有完全产权
2. 家庭成员拥有部分产权
3. 公房（单位提供的房子）
4. 廉租房
5. 公租房
6. 市场上租的商品房
7. 亲戚、朋友的房子
77.其他【请注明】 FQ2SP“其他归属，请注明”


Q801 FQ801“现住房面积（平方米） ”您家现住房的建筑面积是多少平方米？ 1.0 ..
10,000.0 平方米。
F1：“建筑面积”是指供人居住使用的房屋建筑面积。住宅建筑面积测算方式因楼层、结构等
的不同而不同，以房产证、租房合同为准。自建房屋可做简单的丈量，住宅建筑面积＝ 占地的
长× 宽× 楼层数，计量单位为平方米。

Q6 FQ6
元？ 0.01..10,000 万
您估计您家现在居住的这所房子当前的市场总价是多少万
F1：房屋当前市场总价指的是房屋及其所在的宅基地（仅针对农村）转让所能够获得的收
益。

HZ1FHZ1"居住房屋类型"受访家户居住房屋的类型是什么？
1. 单元房 2. 平房 3. 四合院 4. 别墅 5. 联排别墅
6. 小楼房 77. 其他【请注明】

*/


*- 重命名房屋变量
rename (fq2 fq801 fq6 fhz1 fq4 fq3pid_a_1 fq3pid_a_2 fq3pid_a_3 fq3pid_a_4 fq3pid_a_5 fq3pid_a_6 fq3pid_a_7 fq3pid_a_8 fq8) (chanquan area value type house_time owner_1 owner_2 owner_3 owner_4 owner_5 owner_6 owner_7 owner_8 change)
* 重命名收入变量
ren (fincome1 fincome1_per)(income income_per)
keep fid18 fid16 fid14 fid12 fid10 chanquan area value type house_time owner_1 owner_2 owner_3 owner_4 owner_5 owner_6 owner_7 owner_8 change income income_per fid18 fid16 fid14 fid12 fid10 

save "$temp_data/house_2018.dta", replace

*- 婚姻和房产数据集以fid合并
use "$temp_data/adult_2018.dta", clear
drop _merge
merge m:1 fid18 using "$temp_data/house_2018.dta" 

*- 只保留合并成功的项目
keep if _merge==3
drop _merge
 drop if pid==.
 ren * *18
ren (pid18 fid1018 fid1218 fid1418 fid1618 fid1818) (pid fid10 fid12 fid14 fid16 fid18)

*- 储存数据
save "$temp_data/data_2018.dta", replace

*- 合并2018婚姻数据和2010-2016年数据

use "$temp_data/data_2016_2010.dta", clear

merge m:1 pid using "$temp_data/data_2018.dta"

/*
    Result                           # of obs.
    -----------------------------------------
    not matched                        14,655
        from master                     9,393  (_merge==1)
        from using                      5,262  (_merge==2)

    matched                            19,065  (_merge==3)
    -----------------------------------------


master only: 只出现在2010-016年数据中，没有参与2018年调查。但可以通过marriage16判断2016年结婚情况。
using only：只出现在2018年数据中，个体没参与2016年调查

如果是master only，且结婚年份不知道，则删除，因为不知道是否2010年以前结婚的
*/


drop _merge
save "$temp_data/data_2018_2010.dta", replace


*- 处理2020年数据
* 2020年配偶数据
use "$root/CFPS1020/CFPS2020/cfps2020famconf_202306.dta", clear
rename (pid_a_s tb2_a_p tb3_a20_p tb3_a20_s) (pid_s gender marriage marriage_s)
keep if marriage == 2 | marriage == 4
keep fid20 fid18 fid16 fid14 fid12 fid10 pid pid_s gender marriage marriage_s
save "$temp_data/fam_2020.dta", replace

*- 2020年婚姻数据
use "$root/CFPS1020/CFPS2020/cfps2020person_202306.dta", clear

* 重命名婚姻相关变量
rename (qea0 eeb202y eeb202m eeb202c eeb301 qea205y qea205m)(current_status20 orginal_divorce_y orginal_divorce_m if_original original_reason marriage_y marriage_m)
// qea1 是确认，不用)
// 1.未婚 2.有配偶（在婚） 3.同居 4.离婚 5.丧偶
 
* 重命名户口
ren (qa301 qa302a_code qa603)(hukou_type hukou hukou_birth_type)

* 只保留在婚和离婚的个体，且非丧偶
keep if current_status ==2 | current_status == 4
keep if original_reason != 2

* 只保留所需列
keep pid cyear fid10 fid12 fid14 fid16 fid18 fid20 fid_base current_status if_original orginal_divorce_y orginal_divorce_m marriage_y marriage_m original_reason hukou_type hukou hukou_birth_type

save "$temp_data/adult_2020_temp.dta", replace

* adult_2020_temp.dta与fam_2020.dta的pid合并
use "$temp_data/adult_2020_temp.dta",clear
ren * *_s
ren (fid20_s fid18_s fid16_s fid14_s fid12_s fid10_s) (fid20 fid18 fid16 fid14 fid12 fid10)
save "$temp_data/adult_2020_temp_s.dta", replace

use "$temp_data/fam_2020.dta"
merge 1:1 pid using "$temp_data/adult_2020_temp.dta", force
drop _merge
merge m:1 pid_s using "$temp_data/adult_2020_temp_s.dta", force
drop _merge
save "$temp_data/adult_2020.dta", replace

*- 2020年收入和房产数据
use "$root/CFPS1020/CFPS2020/cfps2020famecon_202306.dta", clear

/*

Q2 FQ2“您家现住房归谁所有”您家现在住的房子归谁所有？
1. 家庭成员拥有完全产权
2. 家庭成员拥有部分产权
3. 公房（单位提供的房子）
4. 廉租房
5. 公租房
6. 市场上租的商品房
7. 亲戚、朋友的房子
77.其他【请注明】 FQ2SP“其他归属，请注明”


Q801 FQ801“现住房面积（平方米） ”您家现住房的建筑面积是多少平方米？ 1.0 ..
10,000.0 平方米。
F1：“建筑面积”是指供人居住使用的房屋建筑面积。住宅建筑面积测算方式因楼层、结构等
的不同而不同，以房产证、租房合同为准。自建房屋可做简单的丈量，住宅建筑面积＝ 占地的
长× 宽× 楼层数，计量单位为平方米。

Q6 FQ6
元？ 0.01..10,000 万
您估计您家现在居住的这所房子当前的市场总价是多少万
F1：房屋当前市场总价指的是房屋及其所在的宅基地（仅针对农村）转让所能够获得的收
益。

HZ1FHZ1"居住房屋类型"受访家户居住房屋的类型是什么？
1. 单元房 2. 平房 3. 四合院 4. 别墅 5. 联排别墅
6. 小楼房 77. 其他【请注明】

*/


* 重命名房产变量
rename (fq2 fq801 fq6 fhz1 fq4 fq3pid_a_1 fq3pid_a_2 fq3pid_a_3 fq3pid_a_4 fq3pid_a_5 fq3pid_a_6 fq3pid_a_7 fq3pid_a_8 fq3pid_a_9 fq8) (chanquan area value type house_time owner_1 owner_2 owner_3 owner_4 owner_5 owner_6 owner_7 owner_8 owner_9 change)

* 重命名收入变量
ren (fincome1 fincome1_per)(income income_per)
keep fid20 fid18 fid16 fid14 fid12 fid10 chanquan area value type house_time owner_1 owner_2 owner_3 owner_4 owner_5 owner_6 owner_7 owner_8 owner_9 change income income_per
save "$temp_data/house_2020.dta", replace

* 婚姻和房产数据集以fid合并
use "$temp_data/adult_2020.dta", clear
merge m:1 fid20 using "$temp_data/house_2020.dta" 

* 只保留合并成功的项目
keep if _merge==3
drop _merge
ren * *20 
//ren *20_s20 *20_s
ren (pid20 fid2020 fid1820 fid1620 fid1420 fid1220 fid1020)(pid fid20 fid18 fid16 fid14 fid12 fid10)
drop if pid==.
*- 储存数据
save "$temp_data/data_2020.dta", replace

*- 合并
use "$temp_data/data_2018_2010.dta", clear
merge m:1 pid using "$temp_data/data_2020.dta", force

/*
    Result                           # of obs.
    -----------------------------------------
    not matched                        19,458
        from master                    17,695  (_merge==1)
        from using                      1,763  (_merge==2)

    matched                            16,025  (_merge==3)
    -----------------------------------------

*/

* 去掉不必要的变量
drop _merge
replace marriage12 = current_status12 if marriage12 == . & current_status12 != .
drop current_status12

//save "$temp_data/data.dta", replace


* 中间有离婚就算离婚
gen divorce = 0
ren (current_status1818 current_status2020 current_status14_s14) (current_status18  current_status20 current_status14)
egen temp = rowmax(marriage10 current_status14 marriage12 current_status18 marriage16 current_status20)

replace divorce = 1 if temp == 4
drop temp


* 生成统一fid
gen fid=fid20 if fid20 
replace fid = fid18 if fid20 == . & fid18 != -8
replace fid = fid16 if fid == . & fid18 == . & fid16 != .
replace fid = fid14 if fid == . & fid18 == . & fid16 == . & fid14 != . 
replace fid = fid12 if fid == . & fid18 == . & fid16 == . & fid14 == . & fid12 != . 
//没有缺失值

save "$temp_data/data.dta", replace

//  by fid: gen count = _n
 
/*
*- 合并
cd "$temp_data"

* 读取第一个数据文件
use data_2010.dta, clear

* 循环读取并合并其他数据文件
foreach file in "data_2012.dta" "data_2014.dta" "data_2016.dta" "data_2018.dta" "data_2020.dta" {
    append using `file', force
}

* 保存合并后的数据文件
save merged_data.dta, replace


// fid10 家庭样本代码
// cyear 调查年份
// status 婚姻状况

use "$temp_data/merged_data.dta", clear

* 将fid缺失值替换为fid_base
* 提取 cyear=2020 的数据，创建一个包含 pid 和 fid_base 的参考表
keep if cyear == 2020
keep pid fid_base
ren fid_base fid_base_2020
save "$temp_data/temp_data.dta", replace


* 合并参考表，将 2020 年的 fid_base 信息合并到原始数据中
use "$temp_data/merged_data.dta"
keep fid20 fid18 fid16 fid14 fid12 fid10 cyear current_status pid chanquan area value type house_time owner_1 owner_2 owner_3 owner_4 owner_5 owner_6 owner_7 owner_8 owner_9 change current_status if_original orginal_divorce_y orginal_divorce_m fid_base
merge m:1 pid using "$temp_data/temp_data.dta"

* 只保留合并成功的项目
keep if _merge==3
drop _merge

* 用 cyear=2020 的 fid_base 替换所有年份中的 fid 缺失值
replace fid20 = fid_base_2020 if missing(fid20)

* 去掉中间其他fid
drop fid10 fid12 fid14 fid16 fid18 fid20 fid_base

save "$temp_data/merged_data_fid.dta", replace





// * 生成一个新的变量，用于记录每个fid在2010年的area值
// bysort fid10: gen area_2010 = area if cyear == 2010

// * 将area_2010的值扩展到同一个fid的所有年份
// bysort fid10: replace area_2010 = area_2010[_n-1] if missing(area_2010)

// * 替换area为missing的值为2010年的area值
// replace area = area_2010 if change==1

// * 删除辅助变量
// drop area_2010





*/

drop  if gender20 == .
ren gender20 gender
drop gender12 gender14 gender_s14 gender16 
drop fid12 fid10 fid10_s12 fid10_s10 fid1414 fid1214 fid1014 fid14_s14 fid12_s14 fid10_s14 fid16 fid1416 fid1216 fid1016 fid16_s16 fid14_s16 fid12_s16 fid10_s16 fid18 fid14 fid_base20 fid_base_s20 fid
ren fid20 fid


// replace marriage1414=current_status14 if marriage1414==. & current_status14 != .
// replace marriage14=current_status1414 if marriage14==. & current_status1414 != .
// replace marriage10 = current_status10 if marriage10==. & current_status10 != .
// replace marriage16 = current_status1616 if marriage16==. & current_status1616 != .
// replace marriage18 = current_status18 if marriage18==. & current_status18 != .
// replace marriage20 = current_status20 if marriage20==. &  current_status20!=.
// drop current_status10 current_status14 current_status1414 current_status1616 current_status18 current_status20

* 处理结婚年份
* 首先将"不适用"替换为缺失值
foreach var in original_marriage_y12 original_marriage_y_s12 marriage_y10 marriage_y_s10 marriage_y14 marriage_y_s14 marriage_y16 marriage_y_s16 marriage_y18 marriage_y_s18 marriage_y20 marriage_y_s20 {
    replace `var' = . if `var' == -8
}

* 初始化 marriage_y 为缺失值
gen marriage_y = .

* 手动检查并取每个变量中最小的、非缺失且不等于 -8 的值
foreach var in original_marriage_y12 original_marriage_y_s12 marriage_y10 marriage_y_s10 marriage_y14 marriage_y_s14 marriage_y16 marriage_y_s16 marriage_y18 marriage_y_s18 marriage_y20 marriage_y_s20 {
    replace marriage_y = `var' if missing(marriage_y) & !missing(`var') & `var' != -8
}



* 处理结婚月份

foreach var in original_marriage_m12 original_marriage_m_s12 marriage_m10 marriage_m_s10 marriage_m14 marriage_m_s14 marriage_m16 marriage_m_s16 marriage_m18 marriage_m_s18 marriage_m20 marriage_m_s20 {
    replace `var' = . if `var' == -8 | `var' == -1
}

egen nonmiss_count = rownonmiss(original_marriage_m12 original_marriage_m_s12 marriage_m10 marriage_m_s10 marriage_m14 marriage_m_s14 marriage_m16 marriage_m_s16 marriage_m18 marriage_m_s18 marriage_m20 marriage_m_s20)

egen marriage_m_sum = rowtotal (original_marriage_m12 original_marriage_m_s12 marriage_m10 marriage_m_s10 marriage_m14 marriage_m_s14 marriage_m16 marriage_m_s16 marriage_m18 marriage_m_s18 marriage_m20 marriage_m_s20)

gen marriage_m = marriage_m_sum/nonmiss_count

replace marriage_m = round(marriage_m,1)

drop nonmiss_count marriage_m_sum marriage_m
drop original_marriage_y12 original_marriage_y_s12 marriage_y10 marriage_y_s10 marriage_y14 marriage_y_s14 marriage_y16 marriage_y_s16 marriage_y18 marriage_y_s18 marriage_y20 marriage_y_s20 original_marriage_m12 original_marriage_m_s12 marriage_m10 marriage_m_s10 marriage_m14 marriage_m_s14 marriage_m16 marriage_m_s16 marriage_m18 marriage_m_s18 marriage_m20 marriage_m_s20


gen if_divorce = .

* 如果分开理由为离婚，则离婚

replace if_divorce = 1 if original_reason12 == 1 | original_reason_s12 == 1 | original_reason14 == 1 | original_reason_s14 == 1 | original_reason16 == 1 | original_reason_s16 == 1 | original_reason18 == 1 | original_reason_s18 == 1 | original_reason20 == 1 | original_reason_s20 == 1


replace divorce = if_divorce if divorce == 0 & if_divorce == 1
drop if_divorce

drop original_reason12 original_reason_s12 original_reason14 original_reason_s14 original_reason16 original_reason_s16 original_reason18 original_reason_s18 original_reason20 original_reason_s20


* 初始化 divorce_y 和 divorce_m 变量为空
gen divorce_y = .
gen divorce_m = .

replace divorce_y = orginal_divorce_y12 if orginal_divorce_y12 != -8
replace divorce_y = orginal_divorce_y_s12 if missing(divorce_y) & orginal_divorce_y_s12 != -8
replace divorce_y = orginal_divorce_y14 if missing(divorce_y) & orginal_divorce_y14 != -8
replace divorce_y = orginal_divorce_y_s14 if missing(divorce_y) & orginal_divorce_y_s14 != -8
replace divorce_y = orginal_divorce_y16 if missing(divorce_y) & orginal_divorce_y16 != -8
replace divorce_y = orginal_divorce_y_s16 if missing(divorce_y) & orginal_divorce_y_s16 != -8
replace divorce_y = orginal_divorce_y18 if missing(divorce_y) & orginal_divorce_y18 != -8
replace divorce_y = orginal_divorce_y_s18 if missing(divorce_y) & orginal_divorce_y_s18 != -8
replace divorce_y = orginal_divorce_y20 if missing(divorce_y) & orginal_divorce_y20 != -8
replace divorce_y = orginal_divorce_y_s20 if missing(divorce_y) & orginal_divorce_y_s20 != -8


replace divorce_m = orginal_divorce_m12 if orginal_divorce_m12 != -8
replace divorce_m = orginal_divorce_m_s12 if missing(divorce_m) & orginal_divorce_m_s12 != -8
replace divorce_m = orginal_divorce_m14 if missing(divorce_m) & orginal_divorce_m14 != -8
replace divorce_m = orginal_divorce_m_s14 if missing(divorce_m) & orginal_divorce_m_s14 != -8
replace divorce_m = orginal_divorce_m16 if missing(divorce_m) & orginal_divorce_m16 != -8
replace divorce_m = orginal_divorce_m_s16 if missing(divorce_m) & orginal_divorce_m_s16 != -8
replace divorce_m = orginal_divorce_m18 if missing(divorce_m) & orginal_divorce_m18 != -8
replace divorce_m = orginal_divorce_m_s18 if missing(divorce_m) & orginal_divorce_m_s18 != -8
replace divorce_m = orginal_divorce_m20 if missing(divorce_m) & orginal_divorce_m20 != -8
replace divorce_m = orginal_divorce_m_s20 if missing(divorce_m) & orginal_divorce_m_s20 != -8

drop orginal_divorce_y12 orginal_divorce_m12 orginal_divorce_y_s12 orginal_divorce_m_s12 orginal_divorce_y14 orginal_divorce_m14 orginal_divorce_y_s14 orginal_divorce_m_s14 orginal_divorce_y16 orginal_divorce_m16 orginal_divorce_y_s16 orginal_divorce_m_s16 orginal_divorce_y18 orginal_divorce_m18 orginal_divorce_y_s18 orginal_divorce_m_s18 orginal_divorce_y20 orginal_divorce_m20 orginal_divorce_y_s20 orginal_divorce_m_s20


gen provcd = .
* 找到每行中非缺失且相等的值
foreach var in provcd12 provcd_s12 provcd10 provcd_s10 provcd1414 provcd14_s14 provcd16 provcd16_s16 {
    replace provcd = `var' if missing(provcd) & `var' != .
}

* 检查每行是否所有非缺失值相等
replace provcd = . if provcd10 != provcd & provcd10 != . | ///
                   provcd12 != provcd & provcd12 != . | ///
                   provcd1414 != provcd & provcd1414 != . | ///
                   provcd16 != provcd & provcd16 != . | ///
                   provcd_s12 != provcd & provcd_s12 != . | ///
                   provcd_s10 != provcd & provcd_s10 != . | ///
                   provcd14_s14 != provcd & provcd14_s14 != . | ///
                   provcd16_s16 != provcd & provcd16_s16 != .

drop provcd12 provcd_s12 provcd10 provcd_s10 provcd1414 provcd14_s14 provcd16 provcd16_s16   

drop marriage_date_check_s12

ren time10 house_time10

order pid fid gender pid_s10 pid_s12 pid_s14 pid_s16 pid_s18 pid_s20 cyear14 cyear16 cyear18 cyear20 age10 age_s10 age12 age_s12 age14 age_s14 hukou10 hukou_type10 hukou_s10 hukou_type_s10 hukou12 hukou_type12 hukou_s12 hukou_type_s12  hukou14 hukou_type14 hukou_s14 hukou_type_s14  hukou16 hukou_type16 hukou_s16 hukou_type_s16  hukou18 hukou_type18 hukou_s18 hukou_type_s18   hukou20 hukou_type20 hukou_s20 hukou_type_s20 feduc12 feduc_s12 feduc10 feduc_s10 meduc12 meduc_s12 meduc10 meduc_s10 fparty12 fparty_s12 fparty10 fparty_s10 mparty12 mparty_s12 mparty10 mparty_s10 marriage12 marriage10 marriage10_s12 marriage1414 marriage14_s14 marriage1214 marriage12_s14 marriage16 marriage_s16 marriage1416 marriage14_s16 marriage18 marriage_s18 marriage1618 marriage16_s18 marriage20 marriage_s20 area10 area12 area14 area16 area18 area20 value10 value12 value14 value16 value18 value20 house_time10 house_time12 house_time14 house_time16 house_time18 house_time20 income10 income_per10 income12 income_per12 income14 income_per14 income16 income_per16 income18 income_per18 income20 income_per20


gen cyear = .

replace cyear = cyear14 if !missing(cyear14)
replace cyear = cyear16 if missing(cyear) & !missing(cyear16)
replace cyear = cyear18 if missing(cyear) & !missing(cyear18)
replace cyear = cyear20 if missing(cyear) & !missing(cyear20)
replace cyear = cyear_s14 if missing(cyear) & !missing(cyear_s14)
replace cyear = cyear_s16 if missing(cyear) & !missing(cyear_s16)
replace cyear = cyear_s18 if missing(cyear) & !missing(cyear_s18)
replace cyear = cyear_s20 if missing(cyear) & !missing(cyear_s20)

drop cyear14 cyear16 cyear18 cyear20 cyear_s14 cyear_s16 cyear_s18 cyear_s20


gen area = .

replace area = area10 if area10 != 0
replace area = area12 if missing(area) & area12 != 0
replace area = area14 if missing(area) & area14 != 0
replace area = area16 if missing(area) & area16 != 0
replace area = area18 if missing(area) & area18 != 0
replace area = area20 if missing(area) & area20 != 0

drop area10 area12 area14 area16 area18 area20


gen value = .

replace value = value10 if !missing(value10)
replace value = value12 if missing(value) & !missing(value12)
replace value = value14 if missing(value) & !missing(value14)
replace value = value16 if missing(value) & !missing(value16)
replace value = value18 if missing(value) & !missing(value18)
replace value = value20 if missing(value) & !missing(value20)

drop value10 value12 value14 value16 value18 value20 

save "$temp_data/data.dta", replace


// * 按 pid 和 cyear 排序
// sort pid cyear

// * 计算每个 pid 出现的年份数量
// by pid: gen num_years = _N

// * 计算每个 pid 在出现年份中 `if_original` 为 1 的年份数量
// by pid: egen num_ones = total(if_original == 1)

// * 标记满足条件的 pid
// by pid: gen original = (num_ones == num_years)

// // * 删除满足条件的 pid
// // drop if delete_flag == 1

// // * 删除辅助变量
// // drop num_years num_ones delete_flag




// gen total_individuals =1


// * 按年份汇总离婚个体数和总个体数
// collapse (sum) divorcees= divorce total_individuals, by(cyear)

// * 计算离婚率
// gen divorce_rate = divorcees / total_individuals

// // * 显示结果
// // list cyear divorcees total_individuals divorce_rate, noobs

// save yearly_area_divorce_rate.dta, replace

// * 读取汇总后的数据文件
// use "$temp_data/yearly_area_divorce_rate.dta", clear

// * 将area变量添加回数据集
// merge 1:m cyear using "$temp_data/merged_data.dta", keepusing(area)

// * 绘制断点回归图
// rdplot divorce_rate area, c(90) ytitle("离婚率") xtitle("房产面积") title("断点回归分析: 90平方米")

// * 筛选出120平方米以下的房产，去掉outlier
// keep if area <= 120
// rdplot divorce_rate area, c(90) ytitle("离婚率") xtitle("房产面积") title("断点回归分析: 90平方米")






// * 保留有状态变化的个体
// keep if status_change == 1

// * 删除辅助变量
// drop prev_status status_change

* 保存结果
save status_change.dta, replace


