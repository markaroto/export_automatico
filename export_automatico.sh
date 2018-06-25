#!/bin/bash
#********************************************************************************************************************
#* 									                                                                                *
#*                                                                                                                  *
#* SCRIPT: import_automatico.sh                                                                                     *
#* DESCRIÇO: Realiza o exporte e o importe de um schema, Executar o script somente no banco destino.			                                                    *
#*                                                                                                                  *
#* VERSAO: 1.2                                                                                                      *
#*                                                                                                                  *
#* PARAMETROS: $1(Instancia Destino)                                                                                     *
#*             $2(Instancia Origem)                                    									            *
#*             $3 (Schema)											                                                *
#* ATUALIZACOES:                                                                                                    *
#*                                                                                                                  *
#********************************************************************************************************************

#variaveis recebidas pelo script
instancia_destino=$1
instancia_origem=$2
esquema=$3
##Variaveis
host=`hostname | awk -F'.' '{print $1}'`
host1=`echo $host | tr a-z A-Z`
data=`date +%Y%m%d`
caminho_log=/bkp/logs/export_import-${data}.log
caminho_script=/bkp/${instancia_destino}/dpump/
source /home/oracle/.bash_profile
###Variaveis ORACLE #############
ORACLE_BASE=/ora/app/oracle
ORACLE_SID=${instancia_destino}
ORACLE_HOME=$ORACLE_HOME
LD_LIBRARY_PATH=$ORACLE_HOME/lib
PATH=$PATH:$HOME/bin:/usr/bin:/bin:/usr/lib64/X11:/usr/local/bin:/usr/sbin/:/usr/libexec:/opt/ibm/java-s390x-60/jre/bin:$ORACLE_HOME/bin
#Export das variaveis para backup
export ORACLE_BASE
export ORACLE_HOME
export ORACLE_SID
export PATH

#Função gravar_log
function gravar_log(){
    mensagem_log=$1
	#Acionamento do script gravar_log.
	hora=`date +%H:%M:%S`
	data_log=`date +%d-%m-%Y`
	#Gravação do arquivo log
	echo "${data_log} ${hora} ${host1}   ${$} ${instancia_origem} ${instancia_destino} export_automatico - ${mensagem_log}" >> ${caminho_log}
}
function dados_backup(){
	clear
	echo $$
	echo $#
	echo $*
	echo "Bem vindo sistema export";
	echo ""; echo "";
	echo "Este script so podera se executado no Guest linux com a instancia destino";
	gravar_log "iniciando"
	echo "Banco destino:";
	read instancia_destino
	instancia_destino=`echo $instancia_destino | tr  '[:upper:]' '[:lower:]'`
	gravar_log "Banco destino ${instancia_destino}"
	echo "Banco origem:";
	read instancia_origem
	instancia_origem=`echo $instancia_origem | tr  '[:upper:]' '[:lower:]'`
	gravar_log "Banco origem ${instancia_origem}"
	echo "Esquema sera copiado:";
	read esquema
	esquema=`echo $esquema | tr '[:lower:]' '[:upper:]'`
	gravar_log "Banco esquema ${esquema}"
	
}

#############Incio do Programa##############################
######Teste disponibilidade TNS da instancia#####
####O arquivo TNS deve possui as informações das instancias
#Variavel  comando de LOOP
valida_dados=2
valida_banco_destino=n
if [  -z ${esquema} ];then
	while [ $valida_dados != s ];
	do
		dados_backup
		echo "Você deseja copiar o esquema ${esquema} da instancia de origem  ${instancia_origem} para instacia destino ${instancia_destino} ? (s=sim ou n=nao)"		
		read valida_dados
		gravar_log "Você deseja copiar o esquema ${esquema} da instancia de origem  ${instancia_origem} para instacia destino ${instancia_destino} ? (s=sim ou n=nao) R: ${valida_dados}";
		echo "Você esta no guest da instancia ${instancia_destino} ?(s=sim ou n=nao):"		
		read valida_banco_destino
		gravar_log "Você esta no guest da instancia ${instancia_destino} ?(s=sim ou n=nao) R: ${valida_banco_destino}";
		if [ ${valida_banco_destino} != s ];then
			exit
		fi
		echo "Processo ira apagar todos os dados do esquema ${esquema} na instancia ${instancia_destino}. Você tem certeza?(s=sim ou n=nao):"
		read valida_apagar
		gravar_log "Processo ira apagar todos os dados do esquema ${esquema} na instancia ${instancia_destino}. Você tem certeza?(s=sim ou n=nao). R: ${valida_apagar}";
		if [ ${valida_apagar} != s ];then
			exit
		fi
		
	done
fi
ORACLE_SID=${instancia_destino}
export ORACLE_SID
tnsping ${instancia_origem}
if [ $? == 0 ]; then
	#Permissao para criar DBLINK privado
	sqlplus -s / as sysdba <<EOF
GRANT flashback any table to BKP_COPASA;
GRANT CREATE DATABASE LINK TO BKP_COPASA;
EOF
	sqlplus -s system/bd11adm@${instancia_origem} <<EOF
	GRANT flashback any table to BKP_COPASA;	
EOF
	#Criar o DBLINK
	
	sqlplus -s bkp_copasa/copasabkp <<EOF
create database link "${instancia_origem}_exp"
connect to bkp_copasa
identified by "copasabkp"
using '${instancia_origem}';
EOF
	erro=$?		
	#Validação da criação do DBLINK
	if [ $erro == 0 ]; then
		gravar_log "DBLINK criado com sucesso";
		#gravar_log "teste";
		gravar_log "Iniciado processo export do ${esquema} da instancia ${instancia_origem}";
		#Export do schema usando DBLINK
		expdp userid=bkp_copasa/copasabkp directory=BKP_DPUMP dumpfile=expdp_${instancia_origem}_${esquema}.dmp log=expdp_${instancia_origem}_${esquema}.log network_link=${instancia_origem}_exp schemas=${esquema} consistent=y 
		erro=$?
		gravar_log "Export Finalizado com RC=${erro}";		
		#Validação do export com sucesso
		if [ $erro == 0 ]; then
			#Backup do schema antes de apagar
			gravar_log "Iniciado Processo export de backup da instancia ${instancia_destino}";
			expdp userid=bkp_copasa/copasabkp directory=BKP_DPUMP dumpfile=bkp_dpump_${instancia_destino}_${esquema}.dmp log=bkp_${instancia_destino}_${esquema}.log schemas=${esquema} 
			erro=$?
			gravar_log "Export de backup finalizado com RC=${erro}";
			if [ $erro == 0 ]; then
			
				#Derruba todos usuario do schema.
				sqlplus -s / as sysdba <<EOF
Set verify off
SET PAGES 10000
SET LINES 1000
Set Trimspool On
SET heading OFF
spool '${caminho_script}kill_user.sql'
PROMPT set echo on
--descobrir usuario
select 'ALTER SYSTEM KILL SESSION '  || ''' ' || S.sid ||','||S.serial# ||''' ' ||' IMMEDIATE;' 
from v$session S 
where s.username in ('${esquema}');
spool off

set verify on;
ALTER USER ${esquema} ACCOUNT LOCK;
@${caminho_script}kill_user.sql

EOF
				gravar_log "Usuario ${esquema} bloqueado e kill session.";
				#Delete schema do usuario.
				gravar_log "Criando o script com permissões do usuario";
				sqlplus -s / as sysdba <<EOF
Set verify off
SET PAGES 10000
SET LINES 1000
Set Trimspool On
SET heading OFF
spool '${caminho_script}privilegio.sql'
PROMPT set echo on
--Permissoes
select 'Grant '|| privilege|| ' ON ' || grantor || '.' ||table_name  || ' TO ' || grantee || ';'  from DBA_TAB_PRIVS where grantee in('${esquema}')
/
spool off
set verify on;
EOF
				gravar_log "Criando o script com permissões do usuario RC= $?";
				sqlplus -s / as sysdba <<EOF
drop user ${esquema} cascade
/			
EOF
				gravar_log "Drop user usuario com RC= $? ";
				#gravar_log "Limpeza do schema ${esquema} com sucesso.";	
				impdp \"/ as sysdba\" schemas=${esquema} directory=BKP_DPUMP dumpfile=bkp_dpump_${instancia_destino}_${esquema}.dmp include=USER,ROLE_GRANT,DEFAULT_ROLE,SYSTEM_GRANT,TABLESPACE_QUOTA,GRANT log=create_user.log 
				gravar_log "Create user usuario";
				#Importe do schema.	
				gravar_log " Inciciado a importação do ${esquema} na instancia ${instancia_destino}.";
				impdp \"/ as sysdba\" schemas=${esquema} directory=BKP_DPUMP dumpfile=expdp_${instancia_origem}_${esquema}.dmp log=import_${instancia_origem}_${esquema}.log   
				erro=$?
				sqlplus -s / as sysdba <<EOF
@${caminho_script}privilegio.sql
EOF
				gravar_log "Finalizado a importação do ${esquema} na instancia ${instancia_destino} com RC=${erro}";
				gravar_log "Inciciado a copilação de todos objectos invalidos da instancia ${instancia_destino} ";
				sqlplus -s / as sysdba <<EOF
@?/rdbms/admin/utlrp.sql
ALTER USER ${esquema} ACCOUNT UNLOCK;
EOF
				echo $erro
				gravar_log "Finalizado copilação de todos objectos invalidos da instancia ${instancia_destino} RC=$erro";
				gravar_log "Usuario ${esquema} desbloqueado.";
				echo "                            "
				echo "Você deseja excluir os arquivo do processo?(s=sim ou n=nao):"
				read deletaArquivos
				if [ ${deletaArquivos} == 's' ] || [ ${deletaArquivos} == 'S' ];then 
					rm -f ${caminho_script}resultado.sql
					rm -f /bkp/${instancia_destino}/dpump/expdp_${instancia_origem}_${esquema}.dmp
					rm -f ${caminho_script}privilegio.sql
					rm -f ${caminho_script}kill_user.sql
					rm -f /bkp/${instancia_destino}/dpump/bkp_dpump_${instancia_destino}_${esquema}.dmp
				else
					gravar_log "Arquivos deletados"
					echo "Arquivos não deletados"
				fi				
				sqlplus -s system/bd11adm@${instancia_origem} <<EOF
		REVOKE flashback any table from BKP_COPASA;	
EOF
		fi
		else
			gravar_log "Error para realizar EXPORT RC= ${erro}";
		fi		
	else
		gravar_log "Error ao criar DBLINK RC=${erro}";
	fi
else
	gravar_log "TNS do instancia ${instancia_origem} não disponivel";
fi 

sqlplus -s bkp_copasa/copasabkp <<EOF
DROP DATABASE LINK "${instancia_origem}_exp";
EOF
gravar_log "Drop DBLINK ";