#!/bin/bash

MODULE_TO_DELIVER=""
SHIPMENT=""
ZIP_TO_DELIVER_LOCATION=""
PRODUCT_NUMBER=""
ZIP_TO_DELIVER=""
LAST_BUILT_RSTATE=""
MODULE_LBTYPE=""
MODULE_DESIGN_VOB=""
BUILD_DIR="/vobs/eniq/tools/design_build"
DIR_FILE="/vobs/eniq/tools/design_build/eniq_directories.txt"
DM_SCRIPT_AREA=/vobs/dm_eniq/tools/scripts
USER_ID=""
SHIPMENT_CFG="/vobs/eniq/tools/design_build/shipment.cfg"
REASON="CI-DEV"
DUAL=true
EVENTS=false
STATS=false

EVENTSONLY=(
        "eventsui"
        "eventsservices"
        "son_13b"
	"eventsnotification"
        "glassfish"
        "mediation_gateway"
	"eventsupport"
	"GeoServerScr"
	"mediation_gateway_tar"
	"son_13a"
	"son_14a"
        )

STATSONLY=(
        "redback"
        "xml"
        "sasn"
	"3GPP32435"
	"alarmcfg"
	"ebsmanager"
	"afj_manager"
	"asn1"
	"bcd"
	"BusyHour"
	"ct"
	"ebs"
	"helpset_stats"
	"mediation"
	"mediator"
	"nossdb"
	"sasn"
	"statlibs"
	"stfiop"
	"uncompress"
	"xml"
	"minilink"
	"wifi"
	"wifiinventory"
    "twampst"
    "twampm"
    "twamppt"
    "HXMLPsIptnms"
    "HXMLCsIptnms"
    "volte"
    "kpiparser"
    "information_store_parser"
        )


set -- `getopt m:s:u:n:b: $*`
if [ $? != 0 ]; then
    echo $0: getopt error
    exit 2
fi

for i in $*; do
    case $i in
        -m)  MODULE_TO_DELIVER=$2; shift 2;;
        -s)  SHIPMENT=$2; shift 2;;
        -u)  USER_ID=$2; shift 2;;
        -n)  NIGHTLY=$2; shift 2;;
        -b)  BRANCH=$2; shift 2;;
        --)  shift; break;;
    esac
done

function getUserID {

	MODULES=(
        "AdminUI /vobs/eniq/design/plat/admin_ui/dev/admin_ui"
        "export /vobs/eniq/design/plat/mediation/dev/export"
        "dwhmanager /vobs/eniq/design/plat/management/dev/dwh_manager"
        "installer /vobs/eniq/design/plat/installer/dev/installer"
        "techpackide /vobs/eniq/design/plat/tp_ide/dev/tp_ide"
        "common /vobs/eniq/design/plat/common_utilities/dev"
        "engine /vobs/eniq/design/plat/etl_controller/dev/engine"
        "repository /vobs/eniq/design/plat/installer/dev/repository"
        "parser /vobs/eniq/design/plat/adapters/dev/parser"
        "helpset_stats /vobs/eniq/design/plat/admin_ui/dev/helpset_stats"
        "kpiparser /vobs/eniq/design/plat/adapters/dev/KPIparser"
        "volte /vobs/eniq/design/plat/adapters/dev/volteparser"
        "information_store_parser /vobs/eniq/design/plat/adapters/dev/information_store_parser"
        "twampm /vobs/eniq/design/plat/adapters/dev/twampmparser"
        "twamppt /vobs/eniq/design/plat/adapters/dev/twampptparser"
        "twampst /vobs/eniq/design/plat/adapters/dev/twampstparser"
        "minilink /vobs/eniq/design/plat/adapters/dev/minilink"
        "libs /vobs/eniq/design/plat/installer/dev/libs"
        "scheduler /vobs/eniq/design/plat/etl_controller/dev/scheduler"
        "EBS_Manager /vobs/eniq/design/plat/management/dev/ebsmanager"
        "runtime /vobs/eniq/design/plat/installer/dev/runtime"
        "CT /vobs/eniq/design/plat/adapters/dev/ctparser"
        "licensing /vobs/eniq/design/plat/licensing/dev"
        "CSEXPORT /vobs/eniq/design/plat/adapters/dev/csexport"
        "Monitoring_Aggregation /vobs/eniq/design/plat/monitoring_aggregation/dev"
        "Mediation /vobs/eniq/design/plat/mediation/dev/mediation"
        "mediator /vobs/eniq/design/plat/mediation/dev/mediator"
        "Disk_Manager /vobs/eniq/design/plat/management/dev/disk_manager"
        "ASCII /vobs/eniq/design/plat/adapters/dev/ascii"
        "BusyHour /vobs/eniq/design/plat/admin_ui/dev/busyhourcfg"
        "dbbaseline /vobs/eniq/design/plat/installer/dev/dbbaseline"
        "monitoring /vobs/eniq/design/plat/monitoring_aggregation/dev"
        "3GPP32435 /vobs/eniq/design/plat/adapters/dev/3GPP32435"
        "HXMLPsIptnms /vobs/eniq/design/plat/adapters/dev/iptnmsPS"
        "HXMLCsIptnms /vobs/eniq/design/plat/adapters/dev/iptnmsCS"
        "asn1 /vobs/eniq/design/plat/adapters/dev/asn1"
        "MDC /vobs/eniq/design/plat/adapters/dev/mdc"
        "nascii /vobs/eniq/design/plat/adapters/dev/nascii"
        "eascii /vobs/eniq/design/plat/adapters/dev/eascii"
        "nossdb /vobs/eniq/design/plat/adapters/dev/nossdb"
        "omes /vobs/eniq/design/plat/adapters/dev/omes"
        "omes2 /vobs/eniq/design/plat/adapters/dev/omes2"
        "raml /vobs/eniq/design/plat/adapters/dev/raml"
        "sasn /vobs/eniq/design/plat/adapters/dev/sasn"
        "stfiop /vobs/eniq/design/plat/adapters/dev/stfiop"
        "xml /vobs/eniq/design/plat/adapters/dev/xml"
        "uncompress /vobs/eniq/design/plat/mediation/dev/uncompress"
        "alarm /vobs/eniq/design/plat/alarm_module/dev"
        "ebs /vobs/eniq/design/plat/adapters/dev/ebs"
        "ebsmanager /vobs/eniq/design/plat/management/dev/ebsmanager"
        "afj_manager /vobs/eniq/design/plat/management/dev/afj_manager"
        "alarmcfg /vobs/eniq/design/plat/alarm_config_interface/dev/alarmcfg"
        "redback /vobs/eniq/design/plat/adapters/dev/redback"
        "webportal /vobs/eniq/design/web_portal/dev"
        "ebinary /vobs/eniq/design/plat/adapters/dev/ebinary"
        "testhelper /vobs/eniq/tools/testhelper"
        "bcd /vobs/eniq/design/plat/adapters/dev/bcd"
        "SERVICES /vobs/eniq_events/eniq_events_services"
        "Glassfish /vobs/eniq/design/plat/installer/dev/glassfish");

	for i in "${MODULES[@]}"
	do
       		MOD=$( echo $i | nawk -F " " '{print $1}' )
        	VOB=$( echo $i | nawk -F " " '{print $2}' )
        	if [ "$MODULE_TO_DELIVER" = "$MOD" ]
        	then
                	cd $VOB
                	userids=(`cleartool lshist -r -since yesterday -fmt "%u %n %o\n" | perl -nle 'print if s%^(.*/'${BRANCH}'/\d+) checkin%$1%' | nawk -F " " '{print $1}' | head -15`)
                	for u in "${userids[@]}"
                	do
                        	if [[ "$u" != *vob* ]]
                        	then
                                	$USER_ID=$u
                                	break
                        	fi
                	done
        	fi
	done

	if [ "$USER_ID" == "" ]
	then
		echo "No Valid ID to support this delivery"
		exit -1
	fi

}


if [ "$NIGHTLY" = "Y" ]
then
	USER_ID=""
	getUserID
fi


MODULE_LBTYPE=`echo ${MODULE_TO_DELIVER} | tr '[:lower:]' '[:upper:]'`
ZIP_TO_DELIVER_LOCATION=`cat ${DIR_FILE} | sed -n '/^'${MODULE_TO_DELIVER}\ '/p' | cut -f5 -d' '`
MODULE_DESIGN_VOB=`cat ${DIR_FILE} | sed -n '/^'${MODULE_TO_DELIVER}\ '/p' | cut -f3 -d' '`
PRODUCT_NUMBER=`cat ${DIR_FILE} | sed -n '/^'${MODULE_TO_DELIVER}\ '/p' | cut -f2 -d' '`
LAST_BUILT_RSTATE=`cleartool des -s -ahl ${SHIPMENT} lbtype:${MODULE_LBTYPE}@${MODULE_DESIGN_VOB} | perl -ple 's/^->\s+"(.*?)"/$1/'`
if [ "${MODULE_TO_DELIVER}" = "mediation_gateway_tar" ]
then
  ZIP_TO_DELIVER=`ls -lrt ${ZIP_TO_DELIVER_LOCATION}/*${LAST_BUILT_RSTATE}*.tar.gz | tail -1 | awk '{print $9}'`
else
  ZIP_TO_DELIVER=`ls -lrt ${ZIP_TO_DELIVER_LOCATION}/*${LAST_BUILT_RSTATE}*.zip | tail -1 | awk '{print $9}'`
fi

SHIPMENT_LIST=`grep "^${SHIPMENT} " ${BUILD_DIR}/*.cfg | cut -d : -f 2-`
SHIPMENT_LIST=`echo ${SHIPMENT_LIST#* }`


function isSprintClosed {

        sprintClosed=`egrep "^#.*${SHIPMENT}$" /vobs/dm_eniq/tools/etc/baselines_eniq.txt`
        if [ "$sprintClosed" != "" ]
        then
                echo "The Sprint ${SHIPMENT} is closed for deliveries..."
                exit 1

        fi

}


function isModuleEventsOnly {
	for i in "${EVENTSONLY[@]}"
        do
                if [ $MODULE_TO_DELIVER = $i ];
                then
			EVENTS=true
			DUAL=false	
                fi
        done
}

function isModuleStatsOnly {
	for i in "${STATSONLY[@]}"
        do
                if [ $MODULE_TO_DELIVER = $i ];
                then
                        STATS=true
                        DUAL=false
                fi
        done
}

function runDeliverScript {
        isModuleEventsOnly
        isModuleStatsOnly
	isSprintClosed
        cd
	cs=/tmp/v8.$$
	cat <<EOF >$cs
element * CHECKEDOUT
element * AT
element * R6_OSSRC_3PP
element * /main/LATEST
EOF
        cleartool setcs $cs
	rm -f $cs
        cd ${DM_SCRIPT_AREA}
	arr=$(echo ${SHIPMENT_LIST})

        if $DUAL ; then
                for x in $arr
                do
			PROJ_SHIPMENT=`echo ${x} | sed 's/:/ /'`
                        echo "Running Command: ./deliver_eniq -auto ${PROJ_SHIPMENT} ${REASON} N ${USER_ID} ${PRODUCT_NUMBER} NONE ${ZIP_TO_DELIVER}"
                        ./deliver_eniq -auto ${PROJ_SHIPMENT} ${REASON} Y ${USER_ID} ${PRODUCT_NUMBER} NONE ${ZIP_TO_DELIVER}
                done
        elif $STATS ; then
		for x in $arr
		do
			PROJ_SHIPMENT=`echo ${x} | sed 's/:/ /'`
			if [[ $PROJ_SHIPMENT == *stats* ]]
			then
				echo "Running Command: ./deliver_eniq -auto ${PROJ_SHIPMENT} ${REASON} N ${USER_ID} ${PRODUCT_NUMBER} NONE ${ZIP_TO_DELIVER}"
                        	./deliver_eniq -auto ${PROJ_SHIPMENT} ${REASON} Y ${USER_ID} ${PRODUCT_NUMBER} NONE ${ZIP_TO_DELIVER}
			fi
		done
        else
		for x in $arr
                do
                        PROJ_SHIPMENT=`echo ${x} | sed 's/:/ /'`
                        if [[ $PROJ_SHIPMENT == *events* ]]
			then
				 echo "Running Command: ./deliver_eniq -auto ${PROJ_SHIPMENT} ${REASON} N ${USER_ID} ${PRODUCT_NUMBER} NONE ${ZIP_TO_DELIVER}"
                                ./deliver_eniq -auto ${PROJ_SHIPMENT} ${REASON} Y ${USER_ID} ${PRODUCT_NUMBER} NONE ${ZIP_TO_DELIVER}
                        fi
                done
        fi

}

runDeliverScript
