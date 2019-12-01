
%macro get_target(df,threshold,out);
	Proc rank data=&df. Groups=10 out=&df.;
		By hitintl;
		Var probability_1;
		ranks rango;
	run;
	proc sql; 
			create table &thresholds._rules as 
			select t1.hitintl, t1.rango, count(v_application_id) as Aplicaciones,
					min(probability_1) as min,
					max(probability_1) as max,
					sum(ifn(target_completa_v1=0,1,0)) as good,
					sum(ifn(target_completa_v1=1,1,0)) as bad,
					sum(ifn(target_completa_v1=0,1,0)) as undetermined,
					calculated bas / sum(ifn(target_completa_v1 in(0,1),1,0)) as ulr,
					sum(ifn(target_completa_v1=.,1,0) as blindspot,
					sum(ifn(target_completa_v1=.,probability_1,0)) /calculated blindspot as ulr_blindspot,
					calculted ulr / calculated ulr_blindspot as variation_ulr
			from &df. as t1 
			group by 
					hitintl, rango;
	quit;
	data &thresholds._rules; 
		set &threholds._rules;
				simple_parceling=cat(ifc(rango,"else ","")
									," if hitintl=", hitintl," and rango=",rango
									," then simple_parceling=ifn(aleatorio<=",ulr,",1,0);");
				parceling=cat(ifc(rango,"else ","")
									," if hitintl=", hitintl," and rango=",rango
									,"then parceling=ifn(aleatorio<=",variation_ulr
									,"*probability_1,1,0);");
				call symput(cat("rule_simple_",_n_),simple_parceling);
				call symput(cat("rule_complex_",_n_),parceling);
				call symput("n_rules",_n_);
		run;
	data &out.;
			set &df.;
			aleatorio=ranuni(123);
			if missing(target_completa_v1) then do;
			%do item_rule=1 %to &n_rules.;
			&&rule_simple_&item_rule.
			%end;
			end; else simple_parceling=target_completa_v1;
			if missing(target_completa_v1) then do;
			%do item_rule=1 %to &n_rules.;
			&&rule_complex_&item_rule.
			%end;
			end; else parceling= target_completa_v1;
	run;
	proc sql; 
			create table &thresholds. as 
			select t1.hitintl, t1.rango, 
					count(v_application_id) as Aplicaciones,
					min(probability_1) as min,
					max(probability_1) as max,
					sum(ifn(target_completa_v1=0,1,0)) as good,
					sum(ifn(target_completa_v1=1,1,0)) as bad,
					sum(ifn(target_completa_v1=0,1,0)) as undetermined,
					calculated bas / sum(ifn(target_completa_v1 in(0,1),1,0)) as ulr,
					sum(ifn(target_comple_v1=.,1,0) as blindspot,
					sum(ifn(simple_parceling=0 and missing(target_completa_v1),1,0)) as good_blindspot_simple,
					sum(ifn(simple_parceling=1 and missing(target_completa_v1),1,0)) as bad_blindspot_simple,
					sum(ifn(parceling=0 and missing(target_completa_v1),1,0)) as good_blindspot_complex,
					sum(ifn(parceling=1 and missing(target_completa_v1),1,0)) as bad_blindspot_complex,
					sum(ifn(target_completa_v1=.,probability_1,0)) calculated blindspot as ulr_hat_blinspot,
					calculated ULR/calculated ulr_hat_blindspot as variation_ulr 
					calculated bad_blindspot_simple/calculated blindspot as ulr_blindspot_simple,
					calculated bad_blindspot_complex/calculated blindspot as ulr_blindspot_complex,
					sum(ifn(simple_parceling=1,1,0))/sum(ifn(simple_parceling in (0,1),1,0)) as ulr_simple_parceling,
					sum(ifn(parceling=1,1,0))/sum(ifn(parceling in (0,1),1,0)) as ulr_parceling_complex

			from &df. as t1 
			group by 
					hitintl, rango;
	quit;
%mend;



/* OTV*/
proc sql;
		create table otv as select t1.*
				,ifn(t1.status_decantamiento="BOOK",t3.target_completa_v4,t1.target_max_final_v3) as target_completa_v1
				,t2.*
				from spend.intl_bs_pre_post_v2 as t1 
				left join spend.mz_prob_ttd2016_v2 as t2 
				on t1.v_application_id=t2.v_appid_unique
				left join spend.mz_base_final_filtros_v3 as t3 
				on t1.v_application_id=t3.v_application_id
				where t1.vintage<201701
				order by hitintl;
quit;

proc sql;
create table as otv as select t1.*,t2.* from otv as t1 
left join acq.otv1_sample_flags as t2
on t1.v_application_id=t2.v_application_id;
quit;
data otv_sample_parceling;
		 set otv(where=( sampling=1));
		 if missing(probabiliy_1) then probability_1=0.8995;
		 if status_decantamiento="BOOK" and missing(target_comata_v1) then target_completa_v1=2;
run;
data otv_rej;
		set otv_sample_parceling(where=(satatus_decantamiento not ="BOOK"));
run;
%get_target(df=otv_rej,thrsholds=parceling_otv_,rej,otv=otv_rej)

data gio.otv_sample_acq;
		set otv_sample_parceling(where=(satatus_decantamiento="BOOK")) otv_rej;
		if missing(target_completa_v1) then target=parceling;
		else target=target_completa_v1;
		keep v_application_id target;
run;



/* OTV2 */


proc sql;
		create table otv2 as select t1.*
				,ifn(t1.status_decantamiento="BOOK",t3.target_completa_v4,t1.target_max_final_v3) as target_completa_v1
				,t2.*
				from spend.intl_bs_pre_post_v2 as t1 
				left join spend.mz_prob_ttd2018_v2 as t2 
				on t1.v_application_id=t2.v_application_id
				left join spend.mz_base_final_filtros_v3 as t3 
				on t1.v_application_id=t3.v_application_id
				where t1.vintage>=201711
				order by hitintl;
quit;

proc sql;
create table as otv2 as select t1.*,t2.* from otv2 as t1 
left join acq.otv2_sample_flags as t2
on t1.v_application_id=t2.v_application_id;
quit;
data otv2_sample_parceling;
		 set otv2(where=( sampling=1));
		 if missing(probabiliy_1) then probability_1=0.9070;
		 if status_decantamiento="BOOK" and missing(target_comata_v1) then target_completa_v1=2;
run;
data otv2_rej;
		set otv2_sample_parceling(where=(satatus_decantamiento not ="BOOK"));
run;
%get_target(df=otv2_rej,thrsholds=parceling_otv2_,rej,otv=otv2_rej)

data gio.otv2_sample_acq;
		set otv2_sample_parceling(where=(satatus_decantamiento="BOOK")) otv2_rej;
		if missing(target_completa_v1) then target=parceling;
		else target=target_completa_v1;
		keep v_application_id target;
run;


/* DEV */


proc sql;
		create table dev as select t1.*
				,ifn(t1.status_decantamiento="BOOK",t3.target_completa_v4,t1.target_max_final_v3) as target_completa_v1
				,t2.*
				from spend.intl_bs_pre_post_v2 as t1 
				left join spend.mz_prob_ttd2017_v2 as t2 
				on t1.v_application_id=t2.v_appid_unique
				left join spend.mz_base_final_filtros_v3 as t3 
				on t1.v_application_id=t3.v_application_id
				where t1.vintage>201612 and t1.vintage<=201710
				order by hitintl;
quit;

proc sql;
create table as dev as select t1.*,t2.* from dev as t1 
left join acq.dev_sample_flags as t2
on t1.v_application_id=t2.v_application_id;
quit;
data dev_sample_parceling;
		 set dev(where=( sampling=1));
		 if missing(probabiliy_1) then probability_1=0.9070;
		 if status_decantamiento="BOOK" and missing(target_comata_v1) then target_completa_v1=2;
run;
data dev_rej;
		set dev_sample_parceling(where=(satatus_decantamiento not ="BOOK"));
run;
%get_target(df=dev_rej,thrsholds=parceling_dev_,rej,otv=dev_rej)

data gio.dev_sample_acq;
		set dev_sample_parceling(where=(satatus_decantamiento="BOOK")) dev_rej;
		if missing(target_completa_v1) then target=parceling;
		else target=target_completa_v1;
		keep v_application_id target;
run;