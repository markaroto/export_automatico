#!/bin/bash -x

instancia_origem=$1
instancia_destino1=$2
servidor1=$3
instancia_destino2=$4
servidor2=$5

##Variaveis
host=`hostname | awk -F'.' '{print $1}'`
host1=`echo $host | tr a-z A-Z`
data=`date +%Y%m%d`
caminho_log=/bkp/logs/replicar-copagis-${data}.log
caminho_script0=/bkp/${instancia_destino1}/dpump/
caminho_script1=/bkp/${instancia_destino2}/dpump/
caminho_script2=/bkp/${instancia_origem}/dpump/
source /home/oracle/.bash_profile
###Variaveis ORACLE #############
ORACLE_BASE=/ora/app/oracle
ORACLE_SID=${instancia_origem}
ORACLE_HOME=$ORACLE_HOME
LD_LIBRARY_PATH=$ORACLE_HOME/lib
PATH=$PATH:$HOME/bin:/usr/bin:/bin:/usr/lib64/X11:/usr/local/bin:/usr/sbin/:/usr/libexec:/opt/ibm/java-s390x-60/jre/bin:$ORACLE_HOME/bin
#Export das variaveis para backup
export ORACLE_BASE
export ORACLE_HOME
export ORACLE_SID
export PATH

function gravar_log(){
    mensagem_log=$1
	#Acionamento do script gravar_log.
	hora=`date +%H:%M:%S`
	data_log=`date +%d-%m-%Y`
	#Gravação do arquivo log
	echo "${data_log} ${hora} ${host1}   ${$} ${instancia_origem} replicar - ${mensagem_log}" >> ${caminho_log}
}

#Função enviar email
function enviar_email(){
	#Formato de hora no padrão HH:MM:SS
	hora=`date +%H:%M:%S`
	#Formato de data no padrão DD-MM-YYYY
	data_log=`date +%d-%m-%Y`
	#Parametro recebido na chamada da função
	mensagem_email=$1
	#Instancia de Destino
	instancia_email=$2	
	#Acionamento do email
	#Formato de data no padrão DD/MM/YYYY
	data_men=`date +%d/%m/%Y`
	assuntoemail="${host1} - ${instancia_email} - ${instancia_origem} - PID: ${$} - ${data_men} ${hora} "
	${caminho_script}mail.sh "${host1} - ${instancia_email} - ${instancia_origem} - ${mensagem_email} " "${assuntoemail} " 
	
}


function replicar(){
	temp_instancia_destino=$1
	temp_caminho_script=$2
	temp_servidor=$3
	gravar_log "Enviado arquivo via SCP para instancia ${temp_instancia_destino}"
	resultado=`/usr/bin/expect <<EOF
spawn /usr/bin/scp ${caminho_script2}replicar_${instancia_origem}.dmp oracle@${temp_servidor}:${temp_caminho_script}replicar_${instancia_origem}.dmp
expect "password"
send "ora@2011\r"
set timeout 36000 
expect "$ "
EOF`
	resultadotemp=`echo ${resultado}  | grep  "100%" | cut -d " " -f 1`
	if [ ! -z   ${resultadotemp} ]; then
		gravar_log "Enviado arquivo via SCP para instancia ${temp_instancia_destino} com sucesso"
		gravar_log "Limpeza dos dados na instancia ${temp_instancia_destino}"
		sqlplus -s system/bd11adm@${temp_instancia_destino} <<EOF
Set verify off
SET PAGES 10000
SET LINES 1000
Set Trimspool On
SET heading OFF
spool '${caminho_script2}kill_user.sql'
PROMPT set echo on
--descobrir usuario
select 'ALTER SYSTEM KILL SESSION '  || ''' ' || S.sid ||','||S.serial# ||''' ' ||' IMMEDIATE;' 
from v$session S 
where s.username in ('SDE', 'COPAGIS_GDB');
spool off

set verify on;
ALTER USER SDE ACCOUNT LOCK;

ALTER USER COPAGIS_GDB ACCOUNT LOCK;
@${caminho_script2}kill_user.sql

EOF
		
		#Criação do script de limpeza
		sqlplus  -s system/bd11adm@${temp_instancia_destino} <<EOF
					set verify off
					SET PAGES 10000
					SET LINES 1000
					Set Trimspool On
					SET heading OFF
					spool '${caminho_script2}limpa.sql'
					PROMPT set echo on
					--EXCLUIR CONSTRAINTS
					select   'ALTER TABLE '||OWNER||'.'||TABLE_NAME||' DROP CONSTRAINT '||CONSTRAINT_NAME||';' EXCLUIR_CONSTRAINTS
					from     dba_constraints
					where    owner in ('SDE', 'COPAGIS_GDB')
					and      table_name <> 'SDE_BLK_18'
					and      constraint_type = 'R';
					--EXCLUIR SEQUENCES
					select   DISTINCT 'DROP SEQUENCE '||SEQUENCE_OWNER||'.'||SEQUENCE_NAME||';' EXCLUIR_SEQUENCES
					from     dba_sequences
					where    SEQUENCE_OWNER in ('SDE', 'COPAGIS_GDB');
					--EXCLUIR TRIGGERS
					select   DISTINCT 'DROP TRIGGER '||OWNER||'.'||TRIGGER_NAME||';' EXCLUIR_TRIGGERS
					from     dba_triggers
					where    owner in ('SDE', 'COPAGIS_GDB');
					--EXCLUIR TYPES
					select   DISTINCT 'DROP '||TYPE||' '||OWNER||'.'||NAME||';' EXCLUIR_TYPES
					from     dba_source
					where    owner in ('SDE', 'COPAGIS_GDB');
					--LIMPAR TABELAS
					select   'TRUNCATE TABLE '||OWNER||'.'||TABLE_NAME||';' LIMPAR_TABELAS
					from     dba_tables
					where    owner in ('SDE', 'COPAGIS_GDB');
					--EXCLUIR TABELAS
					select   'DROP TABLE '||OWNER||'.'||TABLE_NAME|| ' CASCADE CONSTRAINTS;' EXCLUIR_TABELAS
					from     dba_tables
					where    owner in ('SDE', 'COPAGIS_GDB')
					and      table_name <> 'SDE_BLK_18';
					--excluir OPERATOR
					select 'DROP OPERATOR ' || owner || '.' || operator_name || ' force;' from dba_operators
					where
					owner in ('SDE', 'COPAGIS_GDB');
					--excluir view
					select 'DROP view ' || owner || '.' || view_name || ' ;' from DBA_VIEWS
					where
					owner in ('SDE', 'COPAGIS_GDB');
					set verify on;
					--Excluir o INDEXTYPE sde.st_spatial_index
					select 'DROP INDEXTYPE sde.st_spatial_index FORCE;' from dual;	
					--excluir type
					select   
					DISTINCT 'DROP '||TYPE||' '||OWNER||'.'||NAME||' force;' EXCLUIR_TYPES
					from     dba_source
					where    owner in ('SDE', 'COPAGIS_GDB') and type='TYPE';
					/
EOF
		#Execução do script de limpeza
		sqlplus  -s system/bd11adm@${temp_instancia_destino} <<EOF
					@${caminho_script2}limpa.sql
EOF
		gravar_log "Limpeza dos dados na instancia ${temp_instancia_destino} com suceso"
		gravar_log "Impdp ddos dados na instancia ${temp_instancia_destino}"
		impdp system/bd11adm@${temp_instancia_destino} directory=BKP_DPUMP dumpfile=replicar_${instancia_origem}.dmp logfile=replicar_${instancia_origem}_import.log schemas=SDE,COPAGIS_GDB TABLE_EXISTS_ACTION=TRUNCATE
		erro=$?
		sqlplus  -s system/bd11adm@${temp_instancia_destino} <<EOF
		ALTER USER SDE ACCOUNT UNLOCK;
		
		ALTER USER COPAGIS_GDB ACCOUNT UNLOCK;
EOF
		gravar_log "Impdp dos dados na instancia ${temp_instancia_destino} com RC=${erro}"
		gravar_log "Recopilando objectos para resolver erros do IMPDP RC=0"
		/usr/bin/expect <<EOF
					spawn ssh oracle@${temp_servidor}
					expect "password:"
					send "ora@2011\r"
					set timeout 36000
					expect "$ "
					send "export ORACLE_SID=${temp_instancia_destino} \r"
					expect "$ "
					send "sqlplus / as sysdba \r"
					expect "SQL> "
					send "purge dba_recyclebin; \r"
					set timeout 36000
					expect "SQL> "
					send "@?/rdbms/admin/utlrp.sql \r"
					set timeout 36000
					expect "SQL> "
EOF
	else
		enviar_email "Enviado arquivo via SCP para servidor com falha" "${temp_instancia_destino}"
		gravar_log "Enviado arquivo via SCP para instancia ${temp_instancia_destino} com falha"					
		fi;	
}

tnsping ${instancia_origem}
if [ $? == 0 ]; then	
	atualizada=`sqlplus -s bkp_copasa/copasabkp@${instancia_origem} <<EOF
	SET heading OFF;
	SELECT upper(de_status) FROM COPAGIS_GDB.MIG_CONTROLE WHERE CD_MIGRACAO like 'ORP9' and upper(de_status) like upper('OK');
EOF`
	if [ $atualizada == "OK" ]; then
		gravar_log "A instancia  ${instancia_origem} foi atualizada com sucesso";
		gravar_log "Export da instancia  ${instancia_origem}";
		rm -f ${caminho_script2}replicar_${instancia_origem}.dmp
		expdp userid=bkp_copasa/copasabkp directory=BKP_DPUMP dumpfile=replicar_${instancia_origem}.dmp log=replicar_${instancia_origem}.log schemas=SDE,COPAGIS_GDB exclude=statistics # consistent=y
		erro=$?
		gravar_log "EXPDP concluido com RC=${erro}"
		if [ $erro == 0 ] || [ $erro == 5 ]; then 
			if [ ! -z $instancia_destino1 ] && [ ! -z ${servidor1} ]; then
				replicar "${instancia_destino1}"  "${caminho_script0}"  "${servidor1}"
				gravar_log "Concluido a instancia ${instancia_destino1}"
			fi;
			if [ ! -z ${instancia_destino2} ] && [ ! -z ${instancia_destino2} ]; then
				replicar ${instancia_destino2} ${instancia_destino2} ${servidor2}
				gravar_log "Concluido a instancia ${instancia_destino2}"
			fi;
		else
			enviar_email "EXPDP finalizado com RC=${erro}" "${instancia_destino1}"
			gravar_log "EXPDP finalizado com RC=${erro}"	
		fi
	else
		enviar_email "A instancia não foi atualizada" "${instancia_destino1}"
		gravar_log "A instancia não foi atualizada com sucesso";
	fi;	
else
	enviar_email "TNS da instancia não disponivel" "${instancia_destino1}"
	gravar_log "TNS da instancia ${instancia_destino1} não disponivel";
fi
