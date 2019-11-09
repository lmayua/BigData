package ext.bigdata.spark.limitstream;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * @author 17075417 
 * 运行任务详细实例
 */
public class TaskDetailInfo {

	/**
	 *  源库类型（FTP,MYSQL,DB2）
	 */
	private String datasource_type;
	
	/**
	 * IDE数据源id  
	 */
	private String datasource_ide_id;

	/**
	 *  抽取加载策略(0:全表覆盖,1:追量增加,2:增量更新)
	 */
	private String job_type;

	/**
	 *  源库系统缩写名称
	 */
	private String datasource_sortname;

	/**
	 *  job info表的ID
	 */
	private int job_info_id;

    /**

     *     行分隔符
      */
    private String row_division;

    /**
     *     列分隔符
      */

    private String column_division;

	/**
	 *  源表名
	 */
	private String datasource_tablename;

	/**
	 *  执行日期
	 */
	private String statis_date;

	/**
	 *  数据日期
	 */
	private String data_date;
	
	/**
	 *  ssa层建表表名
	 */
	private String target_tablename_ssa;
	
	/**
	 *  sor建表表名
	 */
	private String target_tablename_sor;
	
	/**
	 *  增量更新用到的全量表表名
	 */
	private String target_tablename_sorE;
	
	/**
	 *  任务执行周期
	 */
	private String job_cycle;
	
	/**
	 *  任务执行状态
	 */
	private int job_status;
	
	/**
	 *  增量日期字段
	 */
	private String job_incr_field;
	
	/**
	 *  增量日期格式
	 */
	private String job_incr_dateformat;
	
	/**
	 *  分区字段
	 */
	private String partition_field;
	
	/**
	 * 是否需要初始化(1:需要,0:不需要)
	 */
	private String job_init_flag;
	/**
	 * 初始化条件
	 */
	private String job_init_condition;

    /**
     * 筛选条件
     */
	private String job_filter;
	/**
	 * 表主键组合
	 */
	private String primary_columns;
	/**
	 *  表的columns信息，格式：`列名` ` + String + comments +’表描述‘。。。。。,
	 */
	//列如：
	private String field_colums_info;

	/**
	 *  错误信息
	 */
	private String error_message;

    public String getRow_division() {
        return row_division;
    }

    public void setRow_division(String row_division) {
        this.row_division = row_division;
    }

    public String getColumn_division() {
        return column_division;
    }

    public void setColumn_division(String column_division) {
        this.column_division = column_division;
    }

    public String getJob_filter() {
        return job_filter;
    }

    public void setJob_filter(String job_filter) {
        this.job_filter = job_filter;
    }
	
	public String getDatasource_type() {
		return datasource_type;
	}

	public void setDatasource_type(String datasource_type) {
		this.datasource_type = datasource_type;
	}

	public String getJob_type() {
		return job_type;
	}

	public void setJob_type(String job_type) {
		this.job_type = job_type;
	}

	public String getDatasource_sortname() {
		return datasource_sortname;
	}

	public void setDatasource_sortname(String datasource_sortname) {
		this.datasource_sortname = datasource_sortname;
	}

	public int getJob_info_id() {
		return job_info_id;
	}

	public void setJob_info_id(int job_info_id) {
		this.job_info_id = job_info_id;
	}

	public String getDatasource_tablename() {
		return datasource_tablename;
	}

	public void setDatasource_tablename(String datasource_tablename) {
		this.datasource_tablename = datasource_tablename;
	}

	public String getStatis_date() {
		return statis_date;
	}

	public void setStatis_date(String statis_date) {
		this.statis_date = statis_date;
	}

	public String getData_date() {
		return data_date;
	}

	public void setData_date(String data_date) {
		this.data_date = data_date;
	}

	public String getTarget_tablename_ssa() {
		return target_tablename_ssa;
	}

	public void setTarget_tablename_ssa(String target_tablename_ssa) {
		this.target_tablename_ssa = target_tablename_ssa;
	}

	public String getTarget_tablename_sor() {
		return target_tablename_sor;
	}

	public void setTarget_tablename_sor(String target_tablename_sor) {
		this.target_tablename_sor = target_tablename_sor;
	}

	public String getTarget_tablename_sorE() {
		return target_tablename_sorE;
	}

	public void setTarget_tablename_sorE(String target_tablename_sorE) {
		this.target_tablename_sorE = target_tablename_sorE;
	}

	public String getJob_cycle() {
		return job_cycle;
	}

	public void setJob_cycle(String job_cycle) {
		this.job_cycle = job_cycle;
	}

	public String getJob_incr_field() {
		return job_incr_field;
	}

	public void setJob_incr_field(String job_incr_field) {
		this.job_incr_field = job_incr_field;
	}

	public String getJob_incr_dateformat() {
		return job_incr_dateformat;
	}

	public void setJob_incr_dateformat(String job_incr_dateformat) {
		this.job_incr_dateformat = job_incr_dateformat;
	}

	public String getPartition_field() {
		return partition_field;
	}

	public void setPartition_field(String partition_field) {
		this.partition_field = partition_field;
	}


	public String getError_message() {
		return error_message;
	}

	public void setError_message(String error_message) {
		this.error_message = error_message;
	}

	public int getJob_status() {
		return job_status;
	}

	public void setJob_status(int job_status) {
		this.job_status = job_status;
	}


	public String getDatasource_ide_id() {
		return datasource_ide_id;
	}

	public void setDatasource_ide_id(String datasource_ide_id) {
		this.datasource_ide_id = datasource_ide_id;
	}

	public String getField_colums_info() {
		return field_colums_info;
	}

	public void setField_colums_info(String field_colums_info) {
		this.field_colums_info = field_colums_info;
	}

	public String getJob_init_flag() {
		return job_init_flag;
	}

	public void setJob_init_flag(String job_init_flag) {
		this.job_init_flag = job_init_flag;
	}

	public String getJob_init_condition() {
		return job_init_condition;
	}

	public void setJob_init_condition(String job_init_condition) {
		this.job_init_condition = job_init_condition;
	}

	public String getPrimary_columns() {
		return primary_columns;
	}

	public void setPrimary_columns(String primary_columns) {
		this.primary_columns = primary_columns;
	}

	public String toString()
	{
		return ToStringBuilder.reflectionToString(this);
	}
}
