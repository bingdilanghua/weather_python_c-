#include <stdlib.h>
#include <string.h>
#include <fstream>
#include <cstring>
#include <ctime>
#include <time.h>
#include <math.h>
#include <vector>
#include <sys/timeb.h>
#include <stdio.h>
#include <iostream>
#include <map>
#include <sstream>

using namespace std;

#include "dcisg.h"
#include "dcidefine.h"
#include <sys/timeb.h> 

char***  g_ParseResultsForReadData(int rec_num, int attr_num, ColAttr *attrs, char *buf)
{
	int i, j;
	// int rowLen = 0;
	char *tmp = NULL;
	char ***dataFormat = NULL;
	if (rec_num < 0 || attr_num < 0)
	{
		printf("error\n");
		return NULL;
	}
	dataFormat = (char ***)malloc(sizeof(char ***)* rec_num);
	for (i = 0; i < rec_num; i++)
	{
		dataFormat[i] = (char **)malloc(sizeof(char **)* attr_num);
		memset(dataFormat[i], 0, sizeof(char **)* attr_num);
	}
	tmp = buf;
	for (i = 0; i < rec_num; i++)
	{
		for (j = 0; j < attr_num; j++)
		{
			dataFormat[i][j] = tmp;
			tmp = tmp + attrs[j].data_size;
		}
	}
	return dataFormat;
}



int main(int argc, char** argv)  
{  
	CDci load_oci_tools;
	char sqlcmd[5000];
	char sqltmp[5000];
	ErrorInfo_t	error;
	ErrorInfo_t	_error;
	int row;
	int col;
	ColAttr	*colattrp = NULL;
	char	*charbuff = NULL;
	char	***data = NULL;
	int retcode;
	char mdate[100];
	char area[100];
	if(argc<3)
	{
		cout << "xxxx mdate area"<<endl;
		return -1;
	}
	strcpy(mdate,argv[1]);
	strcpy(area,argv[2]);
	retcode = load_oci_tools.Connect("192.168.9.35", "SYSDBA", "SYSDBA", &_error);
	if (!retcode)
	{
		cout << "...load_oci_tools is false-!!!..." << endl;
		cout << _error.error_no << endl;
		return 0;
	}
	else
	{
		cout << "database is ok"<<endl;
	}

	//python initialization
	Py_Initialize();  

	if ( !Py_IsInitialized() ) {  
		return -1;  
	}  

	PyRun_SimpleString("import sys");  
	PyRun_SimpleString("sys.path.append('./')");  

	PyObject *pName,*pModule,*pDict,*pFunc,*pArgs,*pReturn;  
	PyObject *pList,*pList2,*pList3,*pList4,*pList5;
	PyObject *pList_2,*pList2_2,*pList3_2,*pList4_2,*pList5_2;
	
	
	pName = PyString_FromString("tempRain");  
	pModule = PyImport_Import(pName);  
	if ( !pModule ) {  
		printf("can't find tempRain.py");  
		getchar();  
		return -1;  
	}  
	pDict = PyModule_GetDict(pModule);  
	if ( !pDict ) {  
		return -1;  
	}  

	pFunc = PyDict_GetItemString(pDict, "Covariance");  
	if ( !pFunc || !PyCallable_Check(pFunc) ) {  
		printf("can't find function [Covariance]");  
		getchar();  
		return -1;  
	}  

	pArgs = PyTuple_New(2);  

	//sql begin
	/*****1******/
	pList = PyList_New(0);
	memset(sqlcmd, 0, 5000);
	sprintf(sqlcmd, "SELECT distinct to_char(time,'YYYYMM') FROM EMSHIS.LOADFOR.autovalue WHERE to_char(time,'YYYY')='%s' and stcd=(select stcd from EMSHIS.LOADFOR.QSTATIONA where stnm='%s') order by to_char(time,'YYYYMM')",mdate,area);
	retcode = load_oci_tools.ReadData(sqlcmd, &row, &col, &colattrp, &charbuff, &error);
	data = g_ParseResultsForReadData(row, col, colattrp, charbuff);
	for(int i = 0 ; i < row;i++)
	{
		PyList_Append(pList, Py_BuildValue("s",(char*)data[0][i])); 
		
	}
	PyTuple_SetItem(pArgs,0,pList);
	
	/*****2*****/	
	pList2 = PyList_New(0);
	pList2_2 = PyList_New(0);
	memset(sqlcmd, 0, 5000);
	sprintf(sqlcmd, "SELECT distinct to_char(time,'YYYYMMDDHH'),rain FROM EMSHIS.LOADFOR.autovalue WHERE to_char(time,'YYYY')='%s' and stcd=(select stcd from EMSHIS.LOADFOR.QSTATIONA where stnm='%s') order by to_char(time,'YYYYMMDDHH')",mdate,area);
	retcode = load_oci_tools.ReadData(sqlcmd, &row, &col, &colattrp, &charbuff, &error);
	data = g_ParseResultsForReadData(row, col, colattrp, charbuff);
	for(int i = 0 ; i < row;i++)
	{
		for(int j = 0; j < col ; j++)
		{
			PyList_Append(pList2, Py_BuildValue("s",(char*)data[i][j])); 
		}
		
		PyList_Append(pList2_2,pList2);	    
	}
	PyTuple_SetItem(pArgs,1,pList2_2);
	
	/*****3*****/		
	pList3 = PyList_New(0);
	pList3_2 = PyList_New(0);
	memset(sqlcmd, 0, 5000);
	sprintf(sqlcmd, "SELECT distinct to_char(time,'YYYYMMDD'),to_char(time,'HH') FROM EMSHIS.LOADFOR.autovalue WHERE to_char(time,'YYYY')='%s' and stcd=(select stcd from EMSHIS.LOADFOR.QSTATIONA where stnm='%s') order by to_char(time,'YYYYMMDD')",mdate,area);
	retcode = load_oci_tools.ReadData(sqlcmd, &row, &col, &colattrp, &charbuff, &error);
	data = g_ParseResultsForReadData(row, col, colattrp, charbuff);
	for(int i = 0 ; i < row;i++)
	{
		for(int j = 0; j < col ; j++)
		{
			PyList_Append(pList3, Py_BuildValue("s",(char*)data[i][j])); 
		}
		
		PyList_Append(pList3_2,pList3);	    
	}
	PyTuple_SetItem(pArgs,2,pList3_2);
	
	/*****4*****/	
		
	pList4 = PyList_New(0);
	pList4_2 = PyList_New(0);
	memset(sqlcmd, 0, 5000);
	sprintf(sqlcmd, "SELECT distinct to_char(time,'YYYYMM'),to_char(time,'YYYYMMDD') FROM EMSHIS.LOADFOR.autovalue WHERE to_char(time,'YYYY')='%s' and stcd=(select stcd from EMSHIS.LOADFOR.QSTATIONA where stnm='%s') order by to_char(time,'YYYYMMDD')",mdate,area);
	retcode = load_oci_tools.ReadData(sqlcmd, &row, &col, &colattrp, &charbuff, &error);
	data = g_ParseResultsForReadData(row, col, colattrp, charbuff);
	for(int i = 0 ; i < row;i++)
	{
		for(int j = 0; j < col ; j++)
		{
			PyList_Append(pList4, Py_BuildValue("s",(char*)data[i][j])); 
		}
		
		PyList_Append(pList4_2,pList4);	    
	}
	PyTuple_SetItem(pArgs,3,pList4_2);
	
	/*****5*****/	
	pList5 = PyList_New(0);
	pList5_2 = PyList_New(0);
	memset(sqlcmd, 0, 5000);
	sprintf(sqlcmd, "SELECT area,mdate");
	for(int i =0;i<25;i++)
	{
		memset(sqltmp,0,sizeof(sqltmp));
		sprintf(sqltmp,",F%s0",i*6);
		strcat(sqlcmd,sqltmp);
	//¿¿
	retcode = load_oci_tools.ReadData(sqlcmd, &row, &col, &colattrp, &charbuff, &error);
	data = g_ParseResultsForReadData(row, col, colattrp, charbuff);
	for(int i = 0 ; i < row;i++)
	{
		for(int j = 0; j < col ; j++)
		{
			PyList_Append(pList2, Py_BuildValue("s",(char*)data[i][j])); 
		}
		
		PyList_Append(pList2_2,pList2);	    
	}
	PyTuple_SetItem(pArgs,1,pList2_2);
	
	
	/*****6*****/	
	pList2 = PyList_New(0);
	pList2_2 = PyList_New(0);
	memset(sqlcmd, 0, 5000);
	sprintf(sqlcmd, "SELECT distinct to_char(time,'YYYYMMDDHH'),rain FROM EMSHIS.LOADFOR.autovalue WHERE to_char(time,'YYYY')='%s' and stcd=(select stcd from EMSHIS.LOADFOR.QSTATIONA where stnm='%s') order by to_char(time,'YYYYMMDDHH')");
	retcode = load_oci_tools.ReadData(sqlcmd, &row, &col, &colattrp, &charbuff, &error);
	data = g_ParseResultsForReadData(row, col, colattrp, charbuff);
	for(int i = 0 ; i < row;i++)
	{
		for(int j = 0; j < col ; j++)
		{
			PyList_Append(pList2, Py_BuildValue("s",(char*)data[i][j])); 
			PyList_Append(pList2, Py_BuildValue("s",(char*)data[i][j]));
		}
		
		PyList_Append(pList2_2,pList2);	    
	}
	PyTuple_SetItem(pArgs,1,pList2_2);

	//return	
	pReturn = PyObject_CallObject(pFunc, pArgs); 
	printf("----------------------\n");  

	int list_len = 0;
	int val_num = 0;
	list_len = PyList_Size(pReturn);
	cout<<"list_len: "<<list_len<<endl;
	for(int i = 0; i < list_len; i++)
	{
		PyObject *list_item = PyList_GetItem(pReturn,i);
		val_num = PyList_Size(list_item);
		cout << "val_num: "<<val_num<<endl;
		for(int index = 0 ; index < val_num ; index++)
		{
			PyObject *list_item_sub = PyList_GetItem(list_item,index);
			str_tmp[i][index] = PyInt_AsLong(list_item_sub);
			Py_DECREF(list_item_sub);

		}
		Py_DECREF(list_item);

	}

	for(int i =0;i < list_len;i++)
	{
		for(int j = 0 ; j < val_num;j++)
		{
			cout << str_tmp[i][j] << "  ";
		}
		cout << endl;

	}

	Py_DECREF(pName);  
	Py_DECREF(pArgs);  
	Py_DECREF(pModule);  

	Py_Finalize();  

	for (int i = 0; i < row; i++)
	{
		free(data[i]);
	}
	free(data);

	if (!load_oci_tools.DisConnect(&_error))
	{
		cout << "...load_oci_tools is close false!" << endl;
		cout << _error.error_no << endl;
		return -1;
	}
	else
	{
		cout << "...load_oci_tools is close  true¦!!" << endl;
	}


	return 0;  
}
