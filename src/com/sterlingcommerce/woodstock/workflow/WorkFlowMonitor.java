package com.sterlingcommerce.woodstock.workflow;

import java.util.*;
import java.sql.*;
import java.io.*;
import java.security.UnrecoverableKeyException;

import com.sterlingcommerce.refactor.PlatformConstants;
import com.sterlingcommerce.woodstock.ops.server.*;
import com.sterlingcommerce.woodstock.util.*;
import com.sterlingcommerce.woodstock.util.frame.*;
import com.sterlingcommerce.woodstock.util.frame.jdbc.Conn;
import com.sterlingcommerce.woodstock.util.frame.jdbc.JDBCService;
import com.sterlingcommerce.woodstock.util.frame.jndi.JNDIService;
import com.sterlingcommerce.woodstock.util.frame.log.LogService;
import com.sterlingcommerce.woodstock.util.event.Event;
import com.sterlingcommerce.woodstock.util.frame.lock.LockManager;
import com.sterlingcommerce.woodstock.util.frame.sequencedlock.SequencedLockManager;
import com.sterlingcommerce.woodstock.services.controller.ServicesControllerImpl;
import com.sterlingcommerce.woodstock.services.*;
import com.sterlingcommerce.woodstock.workflow.activity.*;
import com.sterlingcommerce.woodstock.workflow.engine.*;
import com.sterlingcommerce.woodstock.workflow.bpdeadline.*;
import com.sterlingcommerce.security.control.SCIKeyReference;
import com.sterlingcommerce.security.kcapi.*;
import com.sterlingcommerce.security.provider.SCIRSA;

/**
 * A class that observes, determines and controls the status of one or
 * more Workflow(s). The WorkFlowMonitor used for three purposes:
 *
 * <p>
 * <ul>
 *  <li>to determine a Workflow instance status and/or state</li>
 *  <li>to mark a Workflow instance that requires "additional attention"</li>
 *  <li>to obtain a "snapshot" of the currently active WorkFlow Instances</li>
 * </ul>
 * </p>
 *
 * <p>
 * State<br>
 * <ul>
 *  <li>Active</li>
 *  <li>Complete</li>
 *  <li>Waiting</li>
 *  <li>IO_Waiting</li>
 *  <li>Halting</li>
 *  <li>Halted</li>
 *  <li>Interrupted</li>
 *  <li>Terminated</li>
 * </ul>
 *
 * <p>
 * Status<br>
 * <ul>
 *  <li>Success</li>
 *  <li>Error</li>
 *  <li>Warning</li>
 * </ul>
 * </p>
 * @see WorkFlowContext
 */

public class WorkFlowMonitor {

    private final String myClassName = this.getClass().getName();


    public transient static final int  UNKNOWN     = -1;
    public transient static final int  ACTIVE      =  0;
    public transient static final int  COMPLETE    =  1;
    public transient static final int  TERMINATED  =  2;
    public transient static final int  WAITING     =  3;
    public transient static final int  HALTED      =  4;
    public transient static final int  HALTING     =  5;

    public transient static final int  INTERRUPTED_AUTO = 6;
    public transient static final int  INTERRUPTED_MAN  = 7;

    public transient static final int  ACTIVE_WAITING         =  8;
    public transient static final int  FORCE_TERMINATED       =  9;
    public transient static final int  WAITING_ON_IO     =  10;
    public transient static final int  COMPLETE_OR_TERMINATED = 20;
    public transient static final int  INDEXED_FAILURES = -5;

    private static final int PRECEDENCE_ACTIVE = 0;
    private static final int PRECEDENCE_HALTING = 1;
    private static final int PRECEDENCE_WAITING = 2;
    private static final int PRECEDENCE_WAITING_ON_IO = 3;
    private static final int PRECEDENCE_TERMINATED = 4;
    private static final int PRECEDENCE_INTERRUPTED_MAN = 5;
    private static final int PRECEDENCE_INTERRUPTED_AUTO = 6;
    private static final int PRECEDENCE_HALTED = 7;
    private static final int PRECEDENCE_COMPLETE = 8;
    private static final int PRECEDENCE_ACTIVE_WAITING = 9;


    // private static final int FORCE_TERMINATE_TIME=10000;


    public transient static final String DEFAULT_SERVICENAME =
        "BUSINESS_PROCESS_MARK";

    public transient static final int SYSTEM_SHUTDOWN = 100;


    //Note, the BASIC_STATUS is updated to the OFFSET's below
    //when the process is COMPLETED or TERMINATED to speed searches

    private static final String GET_STATUS_SQL_START =
        "SELECT DISTINCT WORKFLOW_ID, "+
        "       CASE WHEN BASIC_STATUS = "+WorkFlowContext.ERROR+" THEN "+WorkFlowContext.ERROR+" "+
        "            WHEN BASIC_STATUS = "+WorkFlowContext.ERROR_OFFSET+" THEN "+WorkFlowContext.ERROR+" "+
        "            WHEN BASIC_STATUS = "+WorkFlowContext.SYSTEM_ERROR+" THEN "+WorkFlowContext.ERROR+" "+
        "            WHEN BASIC_STATUS = "+WorkFlowContext.SYSTEM_ERROR_OFFSET+" THEN "+WorkFlowContext.ERROR+" "+
        "            WHEN BASIC_STATUS = "+WorkFlowContext.WFE_SYSTEM_ERROR+" THEN "+WorkFlowContext.ERROR+" "+
        "            WHEN BASIC_STATUS = "+WorkFlowContext.WFE_SYSTEM_ERROR_OFFSET+" THEN "+WorkFlowContext.ERROR+" "+
        "            WHEN BASIC_STATUS = "+WorkFlowContext.SERVICE_CONFIG_ERROR+" THEN "+WorkFlowContext.ERROR+" "+
        "            WHEN BASIC_STATUS = "+WorkFlowContext.SERVICE_CONFIG_ERROR_OFFSET+" THEN "+WorkFlowContext.ERROR+" "+
        "            WHEN BASIC_STATUS = "+WorkFlowContext.WF_INTERRUPT_MAN+" THEN "+WorkFlowContext.ERROR+" "+
        "            WHEN BASIC_STATUS = "+WorkFlowContext.WF_INTERRUPT_MAN_OFFSET+" THEN "+WorkFlowContext.ERROR+" "+
        "            WHEN BASIC_STATUS = "+WorkFlowContext.WF_INTERRUPT_AUTO+" THEN "+WorkFlowContext.ERROR+" "+
        "            WHEN BASIC_STATUS = "+WorkFlowContext.WF_INTERRUPT_AUTO_OFFSET+" THEN "+WorkFlowContext.ERROR+" "+
        "            WHEN BASIC_STATUS = "+WorkFlowContext.WARNING+" THEN "+WorkFlowContext.WARNING+" "+
        "            ELSE "+WorkFlowContext.SUCCESS+" "+
        "       END AS STATUS "+
        "  FROM WORKFLOW_CONTEXT "+
        " WHERE WORKFLOW_ID IN (";

    private static final String GET_STATUS_SQL_END = ") ORDER BY WORKFLOW_ID, STATUS DESC";

    private static BPRecoveryProperties bpRecoveryProps = null;

    public static boolean newStateAndStatus = false;
    public static boolean useOracle9iFirstRows = false;
    public static boolean useMSSQLConvert = false;
    public static boolean useInformixSyntax = false;
    public static boolean appentInformix = false;
    public static boolean orderByWorkFlowId = true;
    public static boolean terminatedLock = true;
    public static String dbNoTransPool = null;
    public static String user_id = null;
    public static String serverName = null;
    public static String dbPool = null;

    public static int maxBPsToDisplay = 10000;
    public static final int GET_STATE_FETCH_SIZE = 10;

    static {
        Properties p = Manager.getProperties("jdbcService");
	Properties workflowProperties = Manager.getProperties("workflows");
	String tLock= workflowProperties.getProperty("terminatedLock");
	if (tLock !=null && tLock.equals("false")) {
	    terminatedLock = false;
	}

        String value = null;

        if( p != null ) {
          value = p.getProperty("useNewStateAndStatusLogic");
          if( (value != null ) &&
            (value.trim().equalsIgnoreCase("true") ) ) {
            newStateAndStatus = true;
          }

          value = p.getProperty("useOracle9iFirstRows");
          if( (value != null ) &&
            (value.trim().equalsIgnoreCase("true") ) ) {
            useOracle9iFirstRows = true;
          }

          value = p.getProperty("useMSSQLConvert");
          if( (value != null ) &&
            (value.trim().equalsIgnoreCase("true") ) ) {
            useMSSQLConvert = true;
          }

        }

        Properties q = Manager.getProperties("si_config");
        if(q != null) {
          value = q.getProperty("JDBC_VENDOR");
          if( (value != null ) &&
            (value.trim().equalsIgnoreCase("Informix") ) ) {
            useInformixSyntax = true;
          }
        }

        if(q != null) {
          value = q.getProperty("JDBC_VENDOR");
          if( (value != null ) &&
            (value.trim().equalsIgnoreCase("Oracle9i") ) ) {
            useOracle9iFirstRows = true;
          }
        }

        value = Manager.getProperty("cluster");
        if( (value != null ) &&
          (value.trim().equalsIgnoreCase("true") ) ) {
          orderByWorkFlowId = false;
        }

        bpRecoveryProps = new BPRecoveryProperties();
        p = Manager.getProperties("ADMIN-UI");
        try{
           value = p.getProperty("MaxBPsToDisplay");
           if( value != null) maxBPsToDisplay = Integer.parseInt( value );
        }catch( Exception ex){
           maxBPsToDisplay = 10000;
        }

	dbNoTransPool = Manager.getProperty("dbNoTransPool");
	serverName = Manager.getProperty("servername");
        dbPool = Manager.getProperty("dbPool");

    }


    public WorkFlowMonitor() {}


    /**
     * Determine the status of suggested Workflow Instance.
     *
     * @param workflowId - the Workflow instance to obtain status for.
     * @return the status code of the Workflow instance. -1 if does not exsist.
     */
    public int getStatus (long workflowId, Connection c) {
        int status = UNKNOWN;
        int branchStatus = -1;
        ArrayList branchIdList = null;
        String branchId = null;
        Connection conn = null;
        String msg = null;

        try {
            if (c == null)
                conn  =  Conn.getConnection();
            else
                conn = c;

            branchIdList = getAllBranchIds(workflowId, conn);

            for (Iterator it = branchIdList.iterator(); it.hasNext(); ) {
                branchId = (String)it.next();

                branchStatus = getBranchStatus(branchId, conn, workflowId);

                switch (branchStatus) {
                case WorkFlowContext.ERROR:
                    status = WorkFlowContext.ERROR;
                    break;

                case WorkFlowContext.WARNING:
                    status = WorkFlowContext.WARNING;
                    break;

                case WorkFlowContext.SUCCESS:
                    status = WorkFlowContext.SUCCESS;
                    break;

                default:
                    if (WFGlobals.out.debug) {
/*                        WFGlobals.out.logDebug( "Oops found Uknown"
                                            +" status = "+branchStatus);*/
                        WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "DEB_branchStatus" ,  new Object[]{ "" + branchStatus });
                    }
                    break;
                }

                if (status != WorkFlowContext.SUCCESS)
                    break; //short curcuit on anything but success.
            }

        }  catch(SQLException sqe) {

            msg = myClassName + ".getBranchStatus() caught SQLException.";
/*            WFGlobals.out.logException(msg, sqe);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getBranchStatus" ,  new Object[]{ "" + myClassName }, sqe);
            sqe.printStackTrace();

        }  catch (Exception e) {

            msg = myClassName + ".getBranchStatus() caught Exception.";
/*            WFGlobals.out.logException(msg, e);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getBranchStatus1" ,  new Object[]{ "" + myClassName }, e);
            e.printStackTrace();
        } finally {
                if (conn != null && c == null)
                    Conn.freeConnection(conn);
        }


        return status;
    }


    /**
     * Determine the status code of suggested Branch Identifier.
     *
     * @param branchId - the identifier to obtain status for.
     * @return the status code for the Branch. -1 if does not exsist.
     */
    public int getBranchStatus (String branchId, Connection c, long workflowId) {
        int status = -1;

        Connection conn = null;
        PreparedStatement pstmt = null;
        String msg = null;

        String sql =null;
        try {
            if (c == null)
                conn  =  Conn.getConnection();
            else
                conn = c;
            sql =JDBCService.getNamedSQL(conn,"wfm_getBranchStatus");
            pstmt = conn.prepareStatement(sql);
            pstmt.setMaxRows(1);
            status = this.getBranchStatus(branchId, pstmt, workflowId);

        }  catch(SQLException sqe) {

            msg = myClassName + ".getBranchStatus() caught SQLException.";
/*            WFGlobals.out.logException(msg, sqe);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getBranchStatus4" ,  new Object[]{ "" + myClassName }, sqe);
            sqe.printStackTrace();

        }  catch (Exception e) {

            msg = myClassName + ".getBranchStatus() caught Exception.";
/*            WFGlobals.out.logException(msg, e);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getBranchStatus11" ,  new Object[]{ "" + myClassName }, e);
            e.printStackTrace();
        } finally {
            try {
                if (pstmt != null)
                    pstmt.close();

                if (conn != null && c == null)
                    Conn.freeConnection(conn);

            } catch (SQLException sqe) {

                msg = myClassName +
                    ".getBranchStatus() caught SQLException "+
                    "while trying to close prepared statement.";
/*                WFGlobals.out.logException(msg, sqe);*/
                WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getBranchStatus2" ,  new Object[]{ "" + myClassName }, sqe);
                sqe.printStackTrace();
            }
        }

        return status;
    }



    /**
     * Determine the status code of suggested Branch Identifier.
     *
     * @param branchId - the identifier to obtain status for.
     * @param pstmt - <tt>PreparedStatement</tt> object to use.
     * @return the status code for the Branch. -1 if does not exsist.
     */
    int getBranchStatus (String branchId, PreparedStatement pstmt, long workflowId) {
        int status = WorkFlowContext.SUCCESS;

        int basicStatus = -1;
        // int wfeStatus = -1;

        ResultSet rs = null;
        String msg = null;

        // if (WFGlobals.out.debug) {
//             WFGlobals.out.logDebug("getBranchStatus() ID = "+ branchId);
        // }

        // IMPORTANT!  Note here that we are getting all of the
        // available status values from the branch except none (the
        // step 0 uses -1 as it's status), success (we default to
        // success if no errors are found), waiting and terminated.
        // the status is really a binary status - success or error.
        // Therefore we weed out the things that are NOT errors
        // (waiting, success, term, none), then evaluate the things
        // that are errors - many kind of errors can exist, we just
        // care about any error in general.  (Even though we have code
        // here for warning, we default to displaying error as its
        // status in the UI because BPML doesn't have the concept of a
        // warning error.

       try {
            pstmt.setLong(1, workflowId);
            pstmt.setString(2, branchId);
            pstmt.setInt(3, WorkFlowContext.SUCCESS);
            pstmt.setInt(4, WorkFlowContext.WAITING);
            pstmt.setInt(5, WorkFlowContext.WAITING_ON_IO);
            pstmt.setInt(6, WorkFlowContext.WF_TERMINATED);
            pstmt.setInt(7, WorkFlowContext.NONE);
            pstmt.setInt(8, WorkFlowContext.WAITING_OFFSET);
            pstmt.setInt(9, WorkFlowContext.WAITING_ON_IO_OFFSET);
            rs = pstmt.executeQuery();

            if (rs != null) {
                while (rs.next()) {
                    basicStatus = rs.getInt("BASIC_STATUS");
                    switch (basicStatus) {
                    case WorkFlowContext.ERROR:
                    case WorkFlowContext.ERROR_OFFSET:
                    case WorkFlowContext.SYSTEM_ERROR:
                    case WorkFlowContext.SYSTEM_ERROR_OFFSET:
                    case WorkFlowContext.WFE_SYSTEM_ERROR:
                    case WorkFlowContext.WFE_SYSTEM_ERROR_OFFSET:
                    case WorkFlowContext.SERVICE_CONFIG_ERROR:
                    case WorkFlowContext.SERVICE_CONFIG_ERROR_OFFSET:
                    case WorkFlowContext.WF_INTERRUPT_MAN:
                    case WorkFlowContext.WF_INTERRUPT_MAN_OFFSET:
                    case WorkFlowContext.WF_INTERRUPT_AUTO:
                    case WorkFlowContext.WF_INTERRUPT_AUTO_OFFSET:
                        status = WorkFlowContext.ERROR;
                        break;

                    case WorkFlowContext.WARNING:
                        status = WorkFlowContext.WARNING;
                        break;

                    default:
                        if (WFGlobals.out.debug) {
/*                            WFGlobals.out.logDebug( "Oops found Unknown"
                                               +" status = "+basicStatus);*/
                            WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "DEB_basicStatus" ,  new Object[]{ "" + basicStatus });
                        }
                        break;
                    }

                    if (status == WorkFlowContext.ERROR)
                        break; //short curcuit on an error.
                }

            } else
                status = WorkFlowContext.SUCCESS;

        } catch(SQLException sqe) {

            msg = myClassName + ".getBranchStatus() caught SQLException.";
/*            WFGlobals.out.logException(msg, sqe);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getBranchStatus5" ,  new Object[]{ "" + myClassName }, sqe);
            sqe.printStackTrace();

        } catch (Exception e) {

            msg = myClassName + ".getBranchStatus() caught Exception.";
/*            WFGlobals.out.logException(msg, e);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getBranchStatus12" ,  new Object[]{ "" + myClassName }, e);
            e.printStackTrace();
        } finally {
            try {
                if (rs != null)
                    rs.close();
            } catch (SQLException sqe) {

                msg = myClassName + ".getBranchStatus() caught SQLException "+
                    "while trying to close prepared statement.";
/*                WFGlobals.out.logException(msg, sqe);*/
                WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getBranchStatus3" ,  new Object[]{ "" + myClassName }, sqe);
                sqe.printStackTrace();
            }
        }

        return status;
    }

    public int getState (String workflowId, Connection c) {

		  if(newStateAndStatus) {
			  return getStateNew(workflowId, c);
	    }
		  else {
			  return getStateOriginal(workflowId, c);
		  }
    }

    public int getStateNew (String workflowId, Connection c) {

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        int state = UNKNOWN;

        String msg = null;

        try {

            if (c == null) {
              conn  =  Conn.getConnection();
            } else {
              conn = c;
            }

            StringBuffer sb = new StringBuffer();
            sb.append( " SELECT ");
            sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_MAIN"));


            sb.append(" WHERE WORKFLOW_ID = ? ");
            sb.append(" AND ACTIVITYINFO_ID = 0 AND STEP_ID = 0 ");

            String sql = sb.toString();

            if(WFGlobals.out.debug) {
              msg = myClassName + ".getStateNew() query " + sql;
/*              WFGlobals.out.logDebug(msg);*/
              WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_msg4" ,  new Object[]{ "" + msg });
            }

            ps = conn.prepareStatement(sql);
            ps.setInt(1, Integer.parseInt(workflowId));
            rs = ps.executeQuery();

            if(rs != null && rs.next()) {
              state = rs.getInt("STATE");
              state = reversePrecedenceOrder(state);
            }


        }  catch(SQLException sqe) {

            msg = myClassName + ".getBranchStatus() caught SQLException.";
/*            WFGlobals.out.logException(msg, sqe);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getBranchStatus6" ,  new Object[]{ "" + myClassName }, sqe);
            sqe.printStackTrace();

        }  catch (Exception e) {

            msg = myClassName + ".getBranchStatus() caught Exception.";
/*            WFGlobals.out.logException(msg, e);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getBranchStatus13" ,  new Object[]{ "" + myClassName }, e);
            e.printStackTrace();
        } finally {
            try {
                if (rs != null)
                    rs.close();

                if (ps != null)
                    ps.close();

                if (conn != null && c == null)
                    Conn.freeConnection(conn);

            } catch (SQLException sqe) {

                msg = myClassName +
                    ".getBranchStatus() caught SQLException "+
                    "while trying to close prepared statement.";
/*                WFGlobals.out.logException(msg, sqe);*/
                WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getBranchStatus21" ,  new Object[]{ "" + myClassName }, sqe);
                sqe.printStackTrace();
            }
        }

        return state;

    }


    public int getStateOriginal (String workflowId, Connection c) {

        long wfId = -1;

        try {
            wfId = Long.parseLong(workflowId);
        } catch (NumberFormatException ne) {

            String msg = myClassName+".getState() caught Exception "+
                "parsing WFID"+workflowId+".";
/*            WFGlobals.out.logException(msg, ne);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_myClassName_getState" ,  new Object[]{ "" + myClassName , "" + workflowId }, ne);
            ne.printStackTrace();
        }

        if (wfId > 0) {
            return getState(wfId, c);
        } else {
            return UNKNOWN;
        }
    }



    /**
     * Determine the state code of suggested WorkFlow Instance Identifier.
     * NOTE: this will be an assesment of all branches together
     * (state precedence TBD)
     *
     * get all branch state and put the state in the sequence order of
     * terminated,interrupted,halted,halting,waiting,active,completed
     * the final state is the highest number in all of the branches.
     * @param workflowId - the identifier to obtain status for.
     * @return the state code for the Branch. -1 if does not exsist.
     */
    public int getState (long wfId, Connection c) {
        int state = UNKNOWN;

        Connection conn = null;
        PreparedStatement pstmt = null;
        String msg = null;

        ResultSet rs = null;
        // ArrayList list = null;
        ArrayList branchList = null;
        // String wfId = null;
        String bid = null;
        int count = 0;
        int bState = UNKNOWN;
        int  stateList[]={-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1};
        boolean newRowsFound = false;
        boolean newBranchFound = false;

        int nextActivityId = -10;
        int activityId = -10;
        int wfeStatus = -10;
        //int stepId = 0;
        //long workflowId = 0;
        int basicStatus = -10;
        String advStatus = null;
        String branchId = null;

        try {

            if (c == null){
                conn  =  Conn.getConnection();
            } else {
                conn = c;
            }

            String newRows = JDBCService.getNamedSQL(conn,"wfm_getState_s");
            String newBranchRows =JDBCService.getNamedSQL(conn,"wfm_getState_s1");
            if (Hint.isHintsEnabled()) {

                pstmt = conn.prepareStatement(newRows);

                pstmt.setLong(1, wfId);
                rs = pstmt.executeQuery();

                if (rs != null) {
                    newRowsFound = true;

                    while (rs.next()) {
                        nextActivityId = rs.getInt("NEXT_AI_ID");
                        activityId = rs.getInt("ACTIVITYINFO_ID");
                        wfeStatus = rs.getInt("WFE_STATUS");
                        basicStatus = rs.getInt("BASIC_STATUS");
                        advStatus = rs.getString("ADV_STATUS");
                        branchId = rs.getString("BRANCH_ID");
                        //System.out.println("\nbranchId("+branchId+
                         //") wfe_status="+wfeStatus+
                        //  " advStatus='"+advStatus+
                        //  "' nextActivityId="+nextActivityId);
                    }
                    if (basicStatus == WorkFlowContext.WAITING) {
                        state = WAITING;
		    } else if (basicStatus == WorkFlowContext.WAITING_ON_IO) {
                        if (nextActivityId == ActivityInfo.DONE) {
                            state = COMPLETE;
                        } else {
                            state = WAITING_ON_IO;
                            }
                    } else if (basicStatus == WorkFlowContext.WF_INTERRUPT_AUTO) {
                        if (nextActivityId == ActivityInfo.DONE) {
                            state = COMPLETE;
                        } else {
                            state = INTERRUPTED_AUTO;
                            }
                    } else if (basicStatus == WorkFlowContext.WF_INTERRUPT_MAN) {
                        if (nextActivityId == ActivityInfo.DONE) {
                            state = COMPLETE;
                        } else {
                            state = INTERRUPTED_MAN;
                            }
                    } else if (basicStatus == WorkFlowContext.WFE_SYSTEM_ERROR ||
                               basicStatus == WorkFlowContext.SYSTEM_ERROR ||
                               basicStatus == WorkFlowContext.SERVICE_CONFIG_ERROR ||
                               basicStatus == WorkFlowContext.WARNING ||
                               basicStatus == WorkFlowContext.ERROR) {
                        if (nextActivityId == ActivityInfo.DONE) {
                            if (wfeStatus==ActivityInfo.CHILD_DONE) {
                                state = HALTED;
                            } else { 
                                state = COMPLETE;
                                }
                        } else if (nextActivityId == activityId) {
                            state = HALTED;
                            //Since when sub workflow is disabled,
                            //activity id=0, and nextacvitity id =1
                        } else if (
                            wfeStatus == WorkFlowContext.WFE_WFD_DEACTIVATED ||
                            wfeStatus == WorkFlowContext.WFE_SERVICE_DEACTIVATED ||
                            wfeStatus == WorkFlowContext.WFE_REMOTE_ERROR ||
                            wfeStatus == WorkFlowContext.WFE_CREATE_ERROR ||
                            wfeStatus == WorkFlowContext.WFE_BASIC_ERROR ||
                            wfeStatus == WorkFlowContext.WFE_NAME_ERROR ||
                            wfeStatus == WorkFlowContext.WFE_LICENSE_ERROR ||
                            wfeStatus == WorkFlowContext.WFE_JMS_ERROR )  {
                               state= HALTED;
                         } else {
                               state = ACTIVE;
                               }
                    } else if (basicStatus == WorkFlowContext.WF_TERMINATED) {
                        state = TERMINATED;
                    } else if (wfeStatus == WorkFlowContext.WFE_DEFAULT_STATUS 
                               && nextActivityId == ActivityInfo.DONE) {
                        state = COMPLETE;
                    } else if (wfeStatus == WorkFlowContext.WFE_DEFAULT_STATUS 
                               && nextActivityId == ActivityInfo.NOOP) {
                        state = ACTIVE_WAITING; // this is a split.
                    } else if (basicStatus ==WFCBase.SUCCESS &&
                              wfeStatus == ActivityInfo.CHILD_DONE) {
                        if (nextActivityId == ActivityInfo.DONE){
                            state = COMPLETE;
                        } else {
                            state = ACTIVE;
                            }
                    } else if (nextActivityId != ActivityInfo.NOOP &&
                               nextActivityId != ActivityInfo.DONE) {
                        if (isWFInactive(wfId, conn)) {
                            state = HALTING;
                        } else {
                            state = ACTIVE;
                        }
                    }
                }

                if (rs != null) {
                    rs.close();
                }
                if (pstmt != null) {
                    pstmt.close();
                }

                pstmt = conn.prepareStatement(newBranchRows);
                pstmt.clearParameters();
                pstmt.setLong(1, wfId);
                rs = pstmt.executeQuery();

                if (rs != null) {
                    newBranchFound = true;
                    while (rs.next()) {
                        nextActivityId = rs.getInt("NEXT_AI_ID");
                        activityId = rs.getInt("ACTIVITYINFO_ID");
                        wfeStatus = rs.getInt("WFE_STATUS");
                        basicStatus = rs.getInt("BASIC_STATUS");
                        advStatus = rs.getString("ADV_STATUS");
                        branchId = rs.getString("BRANCH_ID");
                    //workflowId = rs.getLong("WORKFLOW_ID");
                    //stepId = rs.getInt("STEP_ID");
//                  System.out.println("\nbranchId("+branchId+
//                                     ") wfe_status="+wfeStatus+
//                                     " advStatus='"+advStatus+
//                                     "' nextActivityId="+nextActivityId);
                    }
                }
            }

            if (!newRowsFound && !newBranchFound) {
                branchList = getAllBranchIds(wfId, conn);
                //System.out.println("WfId("+workflowId+") has "+
                //                   branchList.size()+" branch id(s)");
                // int k=0;
                for (Iterator it = branchList.iterator(); it.hasNext(); ) {
                    bid = (String)it.next();
                    bState = getBranchState(bid, conn, wfId);
                    switch (bState)  {
                    case ACTIVE:
                        stateList[10]=ACTIVE;
                        break;
                    case HALTING:
                        stateList[9]=HALTING;
                        break;
                    case WAITING:
                        stateList[8]=WAITING;
                        break;
                    case WAITING_ON_IO:
                        stateList[7]=WAITING_ON_IO;
                        break;
                    case TERMINATED:
                        stateList[6]=TERMINATED;
                        break;
                    case INTERRUPTED_MAN:
                        stateList[5]=INTERRUPTED_MAN;
                        break;
                    case INTERRUPTED_AUTO:
                        stateList[4]=INTERRUPTED_AUTO;
                        break;
                    case HALTED:
                        stateList[3]=HALTED;
                        break;
                    case COMPLETE:
                        stateList[2]=COMPLETE;
                        break;
                    case ACTIVE_WAITING:
                        stateList[1]=ACTIVE;
                        break;
                    default:
                        stateList[0]=-1;
/*                        WFGlobals.out.logError("Cannot find state for this branch. Workflow ID: " + wfId);*/
                        WFGlobals.out.logError( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_Workflow" ,  new Object[]{ "" + wfId });
                        break;
                    }
                    count++;
                }
            }

            for (int j=stateList.length-1; j>0; j--) {
                if (stateList[j]>-1) {
                    state=stateList[j];
                    break; //short cut to the final state
                }
            }
        }  catch(SQLException sqe) {

            msg = myClassName + ".getBranchStatus() caught SQLException.";
/*            WFGlobals.out.logException(msg, sqe);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getBranchStatus7" ,  new Object[]{ "" + myClassName }, sqe);
            sqe.printStackTrace();

        }  catch (Exception e) {

            msg = myClassName + ".getBranchStatus() caught Exception.";
/*            WFGlobals.out.logException(msg, e);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getBranchStatus14" ,  new Object[]{ "" + myClassName }, e);
            e.printStackTrace();
        } finally {
            try {
                if (rs != null)
                    rs.close();

                if (pstmt != null)
                    pstmt.close();

                if (conn != null && c == null)
                    Conn.freeConnection(conn);

            } catch (SQLException sqe) {

                msg = myClassName +
                    ".getBranchStatus() caught SQLException "+
                    "while trying to close prepared statement.";
/*                WFGlobals.out.logException(msg, sqe);*/
                WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getBranchStatus22" ,  new Object[]{ "" + myClassName }, sqe);
                sqe.printStackTrace();
            }
        }

        return state;
    }

    public int getBranchState (String branchId, Connection c, long wfId) {
        return getBranchState (branchId, c, wfId, false);
    }

    /**
     * Determine the state code of suggested Branch Identifier.
     * (state precedence TBD)
     *
     * @param branchId - the identifier to obtain status for.
     * @param c - the JDBC <tt>Connection</tt> object to contact the
     * database via. If <tt>null</tt> a new is obtained from the J2EE
     * container pool.
     * @return the state code for the Branch. -1 if does not exsist.
     */
    public int getBranchState (String branchId, Connection c, long wfId,
                               boolean hints) {
        int state = UNKNOWN;

        // add WORKFLOW_ID and STEP_ID to the read so the index is used
        // even though they are not used for the calculations.
        String sql = null;
        Connection conn = null;
        PreparedStatement pstmt = null;
        String msg = null;

        ResultSet rs = null;

        int nextActivityId = -10;
        int activityId = -10;
        int wfeStatus = -10;
        int basicStatus = -10;
        String advStatus = null;


        try {
            if (c == null) {
                conn  =  Conn.getConnection();
            } else {
                conn = c;
            }
            sql=JDBCService.getNamedSQL(conn,"wfm_getBranchState");
            pstmt = conn.prepareStatement(sql);
            pstmt.setMaxRows(1);

            pstmt.setLong(1, wfId);
            pstmt.setString(2, branchId);
            rs = pstmt.executeQuery();

            if ((rs != null) && rs.next()) {

                nextActivityId = rs.getInt("NEXT_AI_ID");
                activityId = rs.getInt("ACTIVITYINFO_ID");
                wfeStatus = rs.getInt("WFE_STATUS");
                basicStatus = rs.getInt("BASIC_STATUS");
                advStatus = rs.getString("ADV_STATUS");
                //workflowId = rs.getLong("WORKFLOW_ID");
                //stepId = rs.getInt("STEP_ID");
//               System.out.println("\nbranchId("+branchId+")["+stepId+
//                                  "] wfe_status="+wfeStatus+
//                               " advStatus='"+advStatus+
//                                  "' nextActivityId="+nextActivityId);

                if (basicStatus == WorkFlowContext.WAITING) {
                    state = WAITING;
		} else if (basicStatus == WorkFlowContext.WAITING_ON_IO) {
                    state = WAITING_ON_IO;
                } else if (basicStatus == WorkFlowContext.WF_INTERRUPT_AUTO) {
                    if (nextActivityId == ActivityInfo.DONE) {
                        state = COMPLETE;
                    } else {
                        state = INTERRUPTED_AUTO;
                        }
                } else if (basicStatus == WorkFlowContext.WF_INTERRUPT_AUTO_OFFSET &&
                    nextActivityId == ActivityInfo.DONE) {
                  //Once indexing occurs complete and terminated
                  //error fields are offset to increase search performance
                  state = COMPLETE;
                } else if (basicStatus == WorkFlowContext.WF_INTERRUPT_MAN) {
                    if (nextActivityId == ActivityInfo.DONE) {
                        state = COMPLETE;
                    } else {
                        state = INTERRUPTED_MAN;
                        }
                } else if (basicStatus == WorkFlowContext.WF_INTERRUPT_MAN_OFFSET &&
                    nextActivityId == ActivityInfo.DONE) {
                  //Once indexing occurs complete and terminated
                  //error fields are offset to increase search performance
                  state = COMPLETE;
                } else if (basicStatus == WorkFlowContext.WFE_SYSTEM_ERROR ||
                           basicStatus == WorkFlowContext.SYSTEM_ERROR ||
                           basicStatus == WorkFlowContext.SERVICE_CONFIG_ERROR ||
                           basicStatus == WorkFlowContext.WARNING ||
                           basicStatus == WorkFlowContext.ERROR) {
                    if (nextActivityId == ActivityInfo.DONE) {
                        if (wfeStatus == ActivityInfo.CHILD_DONE) {
                            state = HALTED;
                        } else {
                            state = COMPLETE;
                            }
                    } else if (nextActivityId == activityId) {
                        state = HALTED;
                        //Since when sub workflow is disabled,
                        //activity id=0, and nextacvitity id =1
                    } else if (
                      wfeStatus == WFCBase.WFE_WF_INSTANCE_STOPPED ||
                      wfeStatus == WorkFlowContext.WFE_WFD_DEACTIVATED ||
                      wfeStatus == WorkFlowContext.WFE_SERVICE_DEACTIVATED ||
                      wfeStatus == WorkFlowContext.WFE_REMOTE_ERROR ||
                      wfeStatus == WorkFlowContext.WFE_CREATE_ERROR ||
                      wfeStatus == WorkFlowContext.WFE_BASIC_ERROR ||
                      wfeStatus == WorkFlowContext.WFE_NAME_ERROR ||
                      wfeStatus == WorkFlowContext.WFE_LICENSE_ERROR ||
                      wfeStatus == WorkFlowContext.WFE_JMS_ERROR )  {
                        state= HALTED;
                   } else {
                        state = ACTIVE;
                    }
                } else if (
                    ( basicStatus == WorkFlowContext.WFE_SYSTEM_ERROR_OFFSET ||
                      basicStatus == WorkFlowContext.SYSTEM_ERROR_OFFSET ||
                      basicStatus == WorkFlowContext.SERVICE_CONFIG_ERROR_OFFSET ||
                      basicStatus == WorkFlowContext.ERROR_OFFSET ) &&
                    nextActivityId == ActivityInfo.DONE) {
                  //Once indexing occurs complete and terminated
                  //error fields are offset to increase search performance
                  state = COMPLETE;
                } else if (basicStatus == WorkFlowContext.WF_TERMINATED) {
                    state = TERMINATED;
                } else if (wfeStatus == WorkFlowContext.WFE_DEFAULT_STATUS &&
                           nextActivityId == ActivityInfo.DONE) {
                    state = COMPLETE;
                } else if (wfeStatus == WorkFlowContext.WFE_DEFAULT_STATUS &&
                           nextActivityId == ActivityInfo.NOOP) {
                        state = ACTIVE_WAITING; // this is a split.
                } else if (basicStatus ==WFCBase.SUCCESS &&
                           wfeStatus == ActivityInfo.CHILD_DONE) {
                        if (nextActivityId == ActivityInfo.DONE){
                            state = COMPLETE;
                        } else {
                            state = ACTIVE;
                            } 
                } else if (nextActivityId != ActivityInfo.NOOP &&
                    nextActivityId != ActivityInfo.DONE) {

                    if (isWFInactive(wfId, conn)) {
                        state = HALTING;
                    } else {
                        state = ACTIVE;
                    }
                }
            }


            if (hints) {
                Hint hint = new Hint(wfId, branchId, state);
                hint.setWFEStatus(wfeStatus);
                hint.setBasicStatus(basicStatus);
                hint.setAdvStatus(advStatus);
                hint.save(conn);
            }
        }  catch(SQLException sqe) {

            msg = myClassName + ".getBranchStatus() caught SQLException.";
/*            WFGlobals.out.logException(msg, sqe);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getBranchStatus8" ,  new Object[]{ "" + myClassName }, sqe);
            sqe.printStackTrace();

        }  catch (Exception e) {

            msg = myClassName + ".getBranchStatus() caught Exception.";
/*            WFGlobals.out.logException(msg, e);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getBranchStatus15" ,  new Object[]{ "" + myClassName }, e);
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }


                if (pstmt != null) {
                    pstmt.close();
                }

                if (conn != null && c == null) {
                    Conn.freeConnection(conn);
                }
            } catch (SQLException sqe) {

                msg = myClassName +
                    ".getBranchStatus() caught SQLException "+
                    "while trying to close prepared statement.";
/*                WFGlobals.out.logException(msg, sqe);*/
                WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getBranchStatus23" ,  new Object[]{ "" + myClassName }, sqe);
                sqe.printStackTrace();
            }
        }


//      System.out.println("WFId("+wfId+") BId("+branchId+") state = "+state);

        return state;
    }



    private boolean isWFInactive(long workflowId, Connection c)
        throws SQLException {
        boolean flag = false;
        // String msg = null;
        String reasonCode = null;
        String sql =JDBCService.getNamedSQL(c,"wfm_isWFInactive");
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try
        {
           pstmt = c.prepareStatement(sql);
           pstmt.setLong(1, workflowId);
           rs = pstmt.executeQuery();

           if (rs != null) {
              if (rs.next()) {
                 reasonCode = rs.getString("REASON");

                 if (reasonCode != null)
                    flag = true;
              }

              rs.close();
           }
        }
        catch(SQLException sqle)
        {
           throw sqle;
        }
        finally
           {
              if(rs != null)
                 rs.close();

              if (pstmt != null)
                 pstmt.close();
           }

        return flag;
    }

    public ArrayList getAllIdsWithState (Connection con) {

		  if(newStateAndStatus) {
			  return getAllIdsWithStateNew(con, -1);
	    }
		  else {
			  return getAllIdsWithStateOriginal(con);
		  }
	  }

    public ArrayList getAllIdsWithStateNew (Connection con, int num) {


      ArrayList activeList = new ArrayList();
      ArrayList haltingList = new ArrayList();
      ArrayList haltedList = new ArrayList();
      ArrayList waitingList = new ArrayList();
      ArrayList waitingOnIOList = new ArrayList();
      ArrayList interruptedAutoList = new ArrayList();
      ArrayList interruptedManList = new ArrayList();
      ArrayList idList = new ArrayList();

      Connection conn = null;
      ResultSet rs = null;
      PreparedStatement pstmt = null;
      StringBuffer sb = new StringBuffer();
      String sql=null;
      boolean passedIn=false;
      try {
        if ( con ==null) {
            conn = Conn.getConnection();
        } else {
            conn=con;
            passedIn=true;
            }
        sb.append( " SELECT ");
        sb.append(JDBCService.getNamedSQL( conn, "getStateAndStatus_MAIN"));
       	sb.append(" ");
        sb.append(JDBCService.getNamedSQL(conn, "wfm_getAllIdsWithStateNew_where"));
        sql = sb.toString();
        if(WFGlobals.out.debug) {
          String msg = myClassName + ".getIdsWithStateNew() query " + sql;
          WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_msg5" ,  new Object[]{ "" + msg });
          WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "DEB_Size_maxBPsToDisplay" ,  new Object[]{ "" + maxBPsToDisplay });
        }
        pstmt = conn.prepareStatement(sql);
/*
        if(num != -1) {
          pstmt.setFetchSize(num);
        }
        else {
          pstmt.setFetchSize(GET_STATE_FETCH_SIZE);
        }
*/
        rs = pstmt.executeQuery();

        int numFound = 0;

        while(rs != null && rs.next()) {
          String wfId = rs.getString("WORKFLOW_ID");
          int state = rs.getInt("STATE");

          switch (reversePrecedenceOrder(state)) {
            case ACTIVE:                 activeList.add(wfId);          break; 
            case HALTING:                haltingList.add(wfId);         break;
            case HALTED:                 haltedList.add(wfId);          break;
            case WAITING:                waitingList.add(wfId);         break;
            case WAITING_ON_IO:          waitingOnIOList.add(wfId);     break;
            case INTERRUPTED_AUTO:	 interruptedAutoList.add(wfId); break;
            case INTERRUPTED_MAN :       interruptedManList.add(wfId);  break;
            default :                                                   break;
          }
          //idList.add(wfId);
          numFound++;

          if(num != -1 && numFound >= num) {
            //Keep going in the infinite
            //case until result set exhausted or break on n case
            break;
          }
        }

        Hashtable active=new Hashtable();
        active.put("STATE",new Integer (ACTIVE));
        active.put("IDS", activeList);
        idList.add(active);

        Hashtable halted=new Hashtable();
        halted.put("STATE",new Integer (HALTED));
        halted.put("IDS", haltedList);
        idList.add(halted);

        Hashtable halting=new Hashtable();
        halting.put("STATE",new Integer (HALTING));
        halting.put("IDS", haltingList);
        idList.add(halting);

        Hashtable waiting=new Hashtable();
        waiting.put("STATE",new Integer (WAITING));
        waiting.put("IDS", waitingList);
        idList.add(waiting);

        Hashtable waitingOnIO=new Hashtable();
        waitingOnIO.put("STATE",new Integer (WAITING_ON_IO));
        waitingOnIO.put("IDS", waitingOnIOList);
        idList.add(waitingOnIO);

		Hashtable interrupted=new Hashtable();
        interrupted.put("STATE",new Integer (INTERRUPTED_MAN));
        interrupted.put("IDS", interruptedManList);
        idList.add(interrupted);

        Hashtable interruptedAuto=new Hashtable();
        interruptedAuto.put("STATE",new Integer (INTERRUPTED_AUTO));
        interruptedAuto.put("IDS", interruptedAutoList);
        idList.add(interruptedAuto);


      }  catch(SQLException sqe) {
        String msg = myClassName + ".getIdsWithStateNew() caught SQLException.";
        WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getIdsWithStateNew" ,  new Object[]{ "" + myClassName }, sqe);
        sqe.printStackTrace();
      }  catch (Exception e) {
        String msg = myClassName + ".getIdsWithStateNew() caught Exception.";
        WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getIdsWithStateNew1" ,  new Object[]{ "" + myClassName }, e);
        e.printStackTrace();
      } finally {
        try {
          if (rs != null) {
            rs.close();
          }

          if (pstmt != null) {
            pstmt.close();
          }
        } catch (SQLException sqe) {
          String msg = myClassName +
            ".getIdsWithStateNew() caught SQLException" +
            " while trying to close prepared statement.";
          WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getIdsWithStateNew2" ,  new Object[]{ "" + myClassName }, sqe);
          sqe.printStackTrace();
        }

        if (conn != null && !passedIn) {
             Conn.freeConnection(conn);
             }
      }
      return idList;

    }


    /**
     *  get all wf id in diff. live state
     *  it is called from UIGolobas.getActualWorkFlowUsage
     *  all wfs in diff. state will be listed in troubleshotting page
     *
     **/
     public ArrayList getAllIdsWithStateOriginal (Connection con) {
        ArrayList list = null;
        ArrayList idList=new ArrayList();
        ArrayList activeList = new ArrayList();
        ArrayList haltingList = new ArrayList();
        ArrayList haltedList = new ArrayList();
        ArrayList waitingList = new ArrayList();
        ArrayList waitingOnIOList = new ArrayList();
        ArrayList interruptedAutoList = new ArrayList();
        ArrayList interruptedManList = new ArrayList();
        String wfId = null;
        String msg = null;
        boolean passedConn=true;
        int state =-1;

        Connection conn = null;

        try {
            if (conn == null ) {
                conn  =  Conn.getConnection();
                passedConn=false;
            } else {
                conn=con;
                }

            list = getAllIds(conn, -1);
            if (list == null || list.size() ==0) {
                return idList;
            }
            for (Iterator it = list.iterator(); it.hasNext(); ) {
                   wfId = (String)it.next();
                   state=getState(wfId, conn);
                   switch (state) {
                     case ACTIVE:
                       activeList.add(wfId);
                       break;
                     case HALTING:
                       haltingList.add(wfId);
                       break;
                     case HALTED:
                       haltedList.add(wfId);
                       break;
                     case WAITING:
                       waitingList.add(wfId);
                       break;
                     case WAITING_ON_IO:
                       waitingOnIOList.add(wfId);
                       break;
                     case INTERRUPTED_AUTO:
                       interruptedAutoList.add(wfId);
                       break;
                     case INTERRUPTED_MAN:
                       interruptedManList.add(wfId);
                       break;
                     default :
                       break;
                   }
                 }

        Hashtable active=new Hashtable();
        active.put("STATE",new Integer (ACTIVE));
        active.put("IDS", activeList);
        idList.add(active);

        Hashtable halted=new Hashtable();
        halted.put("STATE",new Integer (HALTED));
        halted.put("IDS", haltedList);
        idList.add(halted);

        Hashtable halting=new Hashtable();
        halting.put("STATE",new Integer (HALTING));
        halting.put("IDS", haltingList);
        idList.add(halting);

        Hashtable waiting=new Hashtable();
        waiting.put("STATE",new Integer (WAITING));
        waiting.put("IDS", waitingList);
        idList.add(waiting);

        Hashtable waitingOnIO=new Hashtable();
        waitingOnIO.put("STATE",new Integer (WAITING_ON_IO));
        waitingOnIO.put("IDS", waitingOnIOList);
        idList.add(waitingOnIO);


        Hashtable interrupted=new Hashtable();
        interrupted.put("STATE",new Integer (INTERRUPTED_MAN));
        interrupted.put("IDS", interruptedManList);
        idList.add(interrupted);

        Hashtable interruptedAuto=new Hashtable();
        interruptedAuto.put("STATE",new Integer (INTERRUPTED_AUTO));
        interruptedAuto.put("IDS", interruptedAutoList);
        idList.add(interruptedAuto);


        }  catch(SQLException sqe) {
            msg = myClassName + ".getAllIdsWithState caught SQLException.";
/*            WFGlobals.out.logException(msg, sqe);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getAllIdsWithState" ,  new Object[]{ "" + myClassName }, sqe);
            sqe.printStackTrace();
        }  catch (Exception e) {
            msg = myClassName + ".getAllIdsWithState caught Exception.";
/*            WFGlobals.out.logException(msg, e);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getAllIdsWithState1" ,  new Object[]{ "" + myClassName }, e);
            e.printStackTrace();
        } finally {
            if (conn != null && !passedConn )
                  Conn.freeConnection(conn);
            }


        return idList;
    }

    /**
     * Specialized query for BPMoving, with unlimited resultset size
     * with the suggested state and archive flag.
     *
     * @return <tt>Arraylist</tt> of Workflow Ids that match suggested
     * state. <tt>null</tt> if no matches are located.
     */
    public ArrayList getMovableIds(int state, int archiveFlag) {
        return getMovableIds(state, 0, archiveFlag);
    }



    /**
     * Specialized query for BPMoving. The getMovableIds() will
     * attempt to fetch the suggested number (0 means unlimited) of
     * Workflow IDs so they can be moved from the live system. The
     * only states the getMovableIds() method supports are: <br>
     *   COMPLETE(1),<br>
     *   TERMINATE(2) or<br>
     *   COMPLETE_OR_TERMINATE(20).
     *   INDEXED_FAILURES(-5).
     *
     * @return <tt>Arraylist</tt> of Workflow Ids that match suggested
     * state. <tt>null</tt> if no matches are located.
     */
    public ArrayList getMovableIds (int state, int maxRows, int archiveFlag) {
        ArrayList idList = new ArrayList();
        String wfId = null;
        String msg = null;

        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql  = null;

        try {
            conn  =  Conn.getConnection("local_dbPool");
            if (state == COMPLETE) {
                sql = JDBCService.getNamedSQL(conn,"wfm_getMovableIds_s");
            } else if (state == TERMINATED) {
                sql = JDBCService.getNamedSQL(conn,"wfm_getMovableIds_s1");
            } else if (state == INDEXED_FAILURES) {
                sql = JDBCService.getNamedSQL(conn,"wfm_getMovableIds_s3");
            } else if (state == COMPLETE_OR_TERMINATED) {
                sql = JDBCService.getNamedSQL(conn,"wfm_getMovableIds_s2");
            } else {
/*                WFGlobals.out.logError(myClassName +
                                       ".getMovableIds() unsupported state="+
                                       state);*/
                WFGlobals.out.logError( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getMovableIds" ,  new Object[]{ "" + myClassName , "" + state });
                return null;
            }

            WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "DEB_getMovableIds" ,  new Object[]{ "" + myClassName , "" + state , "" + sql });


            pstmt  = conn.prepareStatement(sql);
            if (state == COMPLETE) {
                pstmt.setInt(1,ActivityInfo.DONE);
                // pstmt.setInt(2,archiveFlag);
                pstmt.setInt(2,WorkFlowContext.WF_TERMINATED);
            } else if (state == TERMINATED) {
                pstmt.setInt(1,WorkFlowContext.WF_TERMINATED);
                // pstmt.setInt(2,archiveFlag);
            } else if (state == INDEXED_FAILURES) {
                pstmt.setTimestamp(1,new Timestamp(System.currentTimeMillis()));
                pstmt.setInt(2,ActivityInfo.DONE);
                pstmt.setInt(3,WorkFlowContext.WF_TERMINATED);
                // pstmt.setInt(3,archiveFlag);
            } else if (state == COMPLETE_OR_TERMINATED) {
                pstmt.setInt(1,ActivityInfo.DONE);
                pstmt.setInt(2,WorkFlowContext.WF_TERMINATED);
                // pstmt.setInt(3,archiveFlag);
               }

            pstmt.setMaxRows(maxRows);
            //rs = pstmt.executeQuery();
            rs = JDBCService.executeQuery(conn,pstmt);

            if (rs != null) {
                while (rs.next()) {
//                    wfId = rs.getString("WORKFLOW_ID");
                    wfId = rs.getString(1);


//                  System.out.println(myClassName+
//                                     "getAllMovableIds  workflow_id = '"+
//                                     wfId+"'");
                    idList.add(wfId);
                }
            }
        }  catch(SQLException sqe) {
            msg = myClassName + ".getMovableIds() caught SQLException. with state ="+state+" sql="+sql;
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getMovableIds1" ,  new Object[]{ "" + myClassName }, sqe);
            sqe.printStackTrace();
        }  catch (Exception e) {
            msg = myClassName + ".getMovableIds() caught Exception.";
/*            WFGlobals.out.logException(msg, e);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getMovableIds2" ,  new Object[]{ "" + myClassName }, e);
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }

                if (pstmt != null) {
                    pstmt.close();
                }
            } catch (SQLException sqe) {
                msg = myClassName + ".getMovableIds(state) caught SQLException"
                    +" while trying to close prepared statement.";
/*                WFGlobals.out.logException(msg, sqe);*/
                WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getMovableIds_state" ,  new Object[]{ "" + myClassName }, sqe);
                sqe.printStackTrace();
            }

            if (conn != null) {
                Conn.freeConnection(conn);
            }
        }


        return idList;
    }

    public ArrayList getAllIds (int state) {
      if(newStateAndStatus) {
        return getAllIdsNew(state);
      }
      else {
        return getAllIdsOriginal(state);
      }
    }

    public ArrayList getAllIdsNew (int state) {

      ArrayList idList = null;

      if (state == SYSTEM_SHUTDOWN) {
        idList = getSystemShutdownList();
      }
      else {
        int [] states = new int[] { state };
        idList = getIdsByStateAndStatus(-1, states, null);
      }

      return idList;
    }

    public ArrayList getSystemShutdownList() {

      ArrayList idList = null;
      PreparedStatement pstmt = null;
      ResultSet rs = null;
      Connection conn = null;
      long wfId = -1;
      String msg = null;
      idList = new ArrayList();
      HashMap b_map=new HashMap();

      try {
          conn  =  Conn.getConnection();
          String sql  =JDBCService.getNamedSQL(conn,"wfm_getSystemShutdownList");
          pstmt=conn.prepareStatement(sql);
          pstmt.setInt(1,WorkFlowContext.WFE_SYSTEM_SHUTDOWN);
          pstmt.setInt(2,PlatformConstants.UNINDEXED_COMPUTE_FLAG);
          rs = pstmt.executeQuery();
          if (rs != null) {
              String b_id=null;
              ArrayList w_info=null;
              int step_id=-1;
              while (rs.next()) {
                  wfId = rs.getLong("WORKFLOW_ID");
                  b_id=rs.getString("BRANCH_ID");
                  step_id=rs.getInt("STEP_ID");
                  w_info=new ArrayList();
                  w_info.add(new Long(wfId));
                  w_info.add(new Integer(step_id));
                  if (!b_map.containsKey(b_id)){
                      b_map.put(b_id,w_info);
                  } else {
                      ArrayList tmp=(ArrayList) b_map.get(b_id);
                      int tmps=((Integer) tmp.get(1)).intValue();
                      if (tmps> step_id) {
                           b_map.put(b_id,w_info);
                           }
                      }
                  }
               }
          if(rs != null)
             rs.close();
          if(pstmt != null)
             pstmt.close();
          if (WFGlobals.out.debug) {
/*              WFGlobals.out.logDebug("shutdown candidate : " +b_map);*/
              WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "DEB_shutdown" ,  new Object[]{ "" + b_map });
              }
          String sql2  =JDBCService.getNamedSQL(conn,"wfm_getSystemShutdownList_s1");
          if (b_map !=null && b_map.size()>0) {
             Set keys = b_map.keySet();
             Iterator it=keys.iterator();
             String b_id=null;
             int i=0;
             int s_id=-1;
             pstmt=conn.prepareStatement(sql2);
             while (it.hasNext()) {
                 b_id =(String)it.next();
                 ArrayList wf_info=(ArrayList) b_map.get(b_id);
                 pstmt.setString(1,b_id);
                 rs = pstmt.executeQuery();
                 if (rs !=null && rs.next()) {
                     String w_Id=((Long) wf_info.get(0)).toString();
                     s_id=rs.getInt("STEP_ID");
                     if (s_id == ((Integer) wf_info.get(1)).intValue()){
                         if (!idList.contains(w_Id)) {
                              idList.add(w_Id);
                              }
                         }
                  }
                  pstmt.clearParameters();
                  i++;
                  }
              }
      }  catch(SQLException sqe) {
            msg = myClassName + ".getSystemShutdownList() caught SQLException.";
/*            WFGlobals.out.logException(msg, sqe);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getSystemShutdownList" ,  new Object[]{ "" + myClassName }, sqe);
            sqe.printStackTrace();
      }  catch (Exception e) {
            msg = myClassName + ".getSystemShutdownList() caught Exception.";
/*            WFGlobals.out.logException(msg, e);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getSystemShutdownList1" ,  new Object[]{ "" + myClassName }, e);
            e.printStackTrace();
      } finally {
          try {
              if (rs != null) { rs.close(); }
              if (pstmt != null) { pstmt.close(); }
          } catch (SQLException sqe) {
              msg = myClassName + ".getSystemShutdownList() caught SQLException "+
                  "while trying to close prepared statement.";
/*              WFGlobals.out.logException(msg, sqe);*/
              WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getSystemShutdownList2" ,  new Object[]{ "" + myClassName }, sqe);
              sqe.printStackTrace();
              }
          if (conn != null) { Conn.freeConnection(conn); }
      }
     if (WFGlobals.out.debug) {
/*          WFGlobals.out.logDebug("shutdownlist " +idList);*/
          WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "DEB_shutdownlist" ,  new Object[]{ "" + idList });
          }
      return idList;
    }

    /**
     * Locate the list of Workflow instance Ids for the suggested state.
     *
     * @param state - the state to locate.
     * @return <tt>Arraylist</tt> of Workflow Ids that match suggested
     * state. <tt>null</tt> if no matches are located.
     */
    public ArrayList getAllIdsOriginal (int state) {

        ArrayList list = null;
        ArrayList idList = new ArrayList();
        String wfId = null;
        String msg = null;

        Connection conn = null;

        try {
            conn  =  Conn.getConnection();

            if (state == SYSTEM_SHUTDOWN) {
                idList = getSystemShutdownList();
            } else {

                list = getAllIds();
                if (list == null || list.size() ==0) {
                    return idList;
                }


                for (Iterator it = list.iterator(); it.hasNext(); ) {
                    wfId = (String)it.next();

                    if (getState(wfId, conn) == state) {
                        idList.add(wfId);
                    }
                }
            }
        }  catch(SQLException sqe) {

            msg = myClassName + ".getAllIds(state) caught SQLException.";
/*            WFGlobals.out.logException(msg, sqe);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getAllIds_state" ,  new Object[]{ "" + myClassName }, sqe);
            sqe.printStackTrace();

        }  catch (Exception e) {

            msg = myClassName + ".getAllIds(state) caught Exception.";
/*            WFGlobals.out.logException(msg, e);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getAllIds_state1" ,  new Object[]{ "" + myClassName }, e);
            e.printStackTrace();
        } finally {
            if (conn != null) {
                Conn.freeConnection(conn);
            }
        }


        return idList;
    }


    public ArrayList getAllIds (int state, int archiveFlag) {
      if(newStateAndStatus) {
        return getAllIdsNew(state, archiveFlag);
      }
      else {
        return getAllIdsOriginal(state, archiveFlag);
      }
    }

    public ArrayList getAllIdsNew (int state, int archiveFlag) {

      int [] states = new int[] { state };
      return getIdsByStateAndStatus(-1, states, null);

    }

    /**
     * Locate the list of Workflow instance Ids for the suggested
     * state and Archive Flag
     *
     * @param state - the state to locate.
     * @return <tt>Arraylist</tt> of Workflow Ids that match suggested
     * state. <tt>null</tt> if no matches are located.
     */
    public ArrayList getAllIdsOriginal (int state, int archiveFlag) {

        // ArrayList list = null;
        ArrayList idList = new ArrayList();
        String wfId = null;
        String msg = null;

        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {
            conn  =  Conn.getConnection();
            ArrayList idArchiveList = null;

            idArchiveList = getAllIds(conn, archiveFlag);

            for (Iterator it = idArchiveList.iterator(); it.hasNext(); ) {
                wfId = (String)it.next();
                if (getState(wfId, conn) == state) {
                    idList.add(wfId);
                }
            }

        }  catch(SQLException sqe) {

            msg = myClassName + ".getAllIds(state) caught SQLException.";
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getAllIds_state4" ,  new Object[]{ "" + myClassName }, sqe);
            sqe.printStackTrace();

        }  catch (Exception e) {

            msg = myClassName + ".getAllIds(state, archiveFlag) caught Exception.";
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getAllIds_state3" ,  new Object[]{ "" + myClassName }, e);
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }

                if (stmt != null) {
                    stmt.close();
                }

                if (conn != null) {
                    Conn.freeConnection(conn);
                    conn = null;
                }
            } catch (SQLException sqe) {

                msg = myClassName + ".getAllIds(state) caught SQLException "+
                    "while trying to close prepared statement.";
                WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getAllIds_state21" ,  new Object[]{ "" + myClassName }, sqe);
                sqe.printStackTrace();
            }
             if (WFGlobals.out.debug) {
              WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "DEB_WFMonitor" ,  new Object[]{ "" + idList });
              }
             if (conn != null) {
                Conn.freeConnection(conn);
                }

        }
        return idList;
    }

    public ArrayList getAllIds (ArrayList list, ArrayList states, ArrayList statuses) {

        ArrayList idList = null;
        String wfId = null;
        String msg = null;
        Connection conn = null;

        try
        {
           conn  =  Conn.getConnection();
           idList = new ArrayList();
           if ((states != null) && (statuses != null))
           {
              for (Iterator it = list.iterator(); it.hasNext(); )
              {
                wfId = (String)it.next();
                // State determination
                int wfState = getState(wfId, conn) ;

                // loop through the list of states, if the wf state
                // equals any of the passed in states then check the
                // wf status to see if it //matches any of the passed
                // in statuses, if so then add it to the list of
                // returned ids
                for (Iterator it2 = states.iterator(); it2.hasNext(); )
                {
                    int state = Integer.parseInt((String)it2.next());
                    if (wfState  == state)
                        {
                            // Status determination
                            int wfStatus = getStatus(Long.parseLong(wfId), conn) ;
                            for (Iterator it3 = statuses.iterator(); it3.hasNext(); )
                                {
                                    int status = Integer.parseInt((String)it3.next());
                                    if (wfStatus  == status)
                                        {
                                            idList.add(wfId);
                                            break;
                                        }
                                }
                            break;
                        }
                }
              }
           }
           else if (states != null)
           {
             for (Iterator it = list.iterator(); it.hasNext(); )
             {
                wfId = (String)it.next();
                // State determination
                int wfState = getState(wfId, conn) ;
                for (Iterator it2 = states.iterator(); it2.hasNext(); )
                {
                        int state = Integer.parseInt((String)it2.next());
                        if (wfState  == state)
                        {
                          idList.add(wfId);
                          break;
                        }
                }
             }
           }
           else if (statuses != null)
           {
             for (Iterator it = list.iterator(); it.hasNext(); )
             {
                wfId = (String)it.next();
                // Status determination
                int wfStatus = getStatus(Integer.parseInt(wfId), conn) ;
                for (Iterator it3 = statuses.iterator(); it3.hasNext(); )
                {
                   int status = Integer.parseInt((String)it3.next());
                   if (wfStatus  == status)
                   {
                        idList.add(wfId);
                        break;
                   }
                }
             }
           }
           else
            idList = list;
        }
        catch(SQLException sqe) {

            msg = myClassName + ".getAllIds() caught SQLException.";
/*            WFGlobals.out.logException(msg, sqe);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_SQLException23" ,  new Object[]{ "" + myClassName }, sqe);
            sqe.printStackTrace();

        }  catch (Exception e) {

            msg = myClassName + ".getAllIds() caught Exception.";
/*            WFGlobals.out.logException(msg, e);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getAllIds" ,  new Object[]{ "" + myClassName }, e);
            e.printStackTrace();
        } finally {
                if (conn != null)
                    Conn.freeConnection(conn);
        }


        return idList;
    }



    /**
     * Locate the list of all Workflow instance Ids.
     *
     * @return <tt>Arraylist</tt> of Workflow Ids.
     * <tt>null</tt> if no matches are located.
     */
    public ArrayList getAllIds () {

        Connection conn = null;
        String msg = null;

        ArrayList list = null;

        try {
            conn  =  Conn.getConnection();
            list = this.getAllIds(conn);

        }  catch(SQLException sqe) {

            msg = myClassName + ".getAllIds() caught SQLException.";
/*            WFGlobals.out.logException(msg, sqe);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_SQLException231" ,  new Object[]{ "" + myClassName }, sqe);
            sqe.printStackTrace();

        }  catch (Exception e) {

            msg = myClassName + ".getAllIds() caught Exception.";
/*            WFGlobals.out.logException(msg, e);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getAllIds1" ,  new Object[]{ "" + myClassName }, e);
            e.printStackTrace();
        } finally {
                if (conn != null)
                    Conn.freeConnection(conn);

        }

        return list;
    }


    /**
     * Locate the list of all Workflow instance Ids.
     *
     * @param conn - JDBC <tt>Connection</tt> to use.
     * @return <tt>Arraylist</tt> of Workflow Ids.
     * <tt>null</tt> if no matches are located.
     */
    ArrayList getAllIds (Connection conn) {

        PreparedStatement pstmt = null;
        ResultSet rs = null;

        ArrayList list = null;

        String sql  = JDBCService.getNamedSQL(conn,"wfm_getAllIds");
        String msg = null;
        String workflowId;

        try {
            pstmt  = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();


            if (rs != null) {
                list = new ArrayList();

                // if (WFGlobals.out.debug) {
//                     WFGlobals.out.logDebug(myClassName + "getAllIds  get rs = '");
                // }
                while (rs.next()) {

                    workflowId = rs.getString("WORKFLOW_ID");


//                  System.out.println(myClassName+
//                                      "getAllIds  workflow_id = '"+
//                                      workflowId+"'");


                    // if (WFGlobals.out.debug) {
//                         WFGlobals.out.logDebug(myClassName+
//                                      "getAllIds  workflow_id = '"+
//                                      workflowId+"'");
                    // }
                    list.add(workflowId);
                }
            }
        } catch(SQLException sqe) {

            msg = myClassName + ".getAllIds() caught SQLException.";
/*            WFGlobals.out.logException(msg, sqe);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_SQLException232" ,  new Object[]{ "" + myClassName }, sqe);
            sqe.printStackTrace();

        } catch (Exception e) {

            msg = myClassName + ".getAllIds(conn) caught Exception.";
/*            WFGlobals.out.logException(msg, e);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getAllIds_conn" ,  new Object[]{ "" + myClassName }, e);
            e.printStackTrace();
        } finally {
            try {
                if (rs != null)
                    rs.close();

                if (pstmt != null)
                    pstmt.close();

            } catch (SQLException sqe) {

                msg = myClassName + ".getAllIds(conn) caught SQLException "+
                    "while trying to close prepared statement.";
/*                WFGlobals.out.logException(msg, sqe);*/
                WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getAllIds_conn1" ,  new Object[]{ "" + myClassName }, sqe);
                sqe.printStackTrace();
            }
        }


        return list;
    }

    ArrayList getAllIds (Connection conn, int archiveFlag) {
        return getAllIds (conn, archiveFlag, true);
    }

    /**
     * Locate the list of all Workflow instance Ids.
     *
     * @param conn - JDBC <tt>Connection</tt> to use.
     * @return <tt>Arraylist</tt> of Workflow Ids.
     * <tt>null</tt> if no matches are located.
     */
    ArrayList getAllIds (Connection conn, int archiveFlag, boolean asStrings) {

        PreparedStatement pstmt = null;
        ResultSet rs = null;

        ArrayList list = null;

        String sql  = JDBCService.getNamedSQL(conn,"wfm_getAllIds_s1");
        String msg = null;
        String workflowId;
        Long wfId = null;


        try {
            pstmt  = conn.prepareStatement(sql);
            pstmt.setInt(1,archiveFlag);
            rs = pstmt.executeQuery();


            if (rs != null) {
                list = new ArrayList();

                while (rs.next()) {

                    if (asStrings) {
                        workflowId = rs.getString("WF_ID");
                        list.add(workflowId);
                    } else {
                        wfId = new Long(rs.getLong("WF_ID"));
                        list.add(wfId);
                    }
                }
            }
        } catch(SQLException sqe) {
            msg = myClassName +
                ".getAllIds(conn, archiveFlag) caught SQLException.";
/*            WFGlobals.out.logException(msg, sqe);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getAllIds_conn2" ,  new Object[]{ "" + myClassName }, sqe);
            sqe.printStackTrace();
        } catch (Exception e) {
            msg = myClassName +
                ".getAllIds(conn, archiveFlag) caught Exception.";
/*            WFGlobals.out.logException(msg, e);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getAllIds_conn3" ,  new Object[]{ "" + myClassName }, e);
            e.printStackTrace();
        } finally {
            try {
                if (rs != null)
                    rs.close();

                if (pstmt != null)
                    pstmt.close();

            } catch (SQLException sqe) {
                msg = myClassName +
                    ".getAllIds(conn, archiveFlag) caught SQLException "+
                    "while trying to close prepared statement.";
/*                WFGlobals.out.logException(msg, sqe);*/
                WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getAllIds_conn4" ,  new Object[]{ "" + myClassName }, sqe);
                sqe.printStackTrace();
            }
        }


        return list;
    }
    /**
     * Determines the number of non indexed BPs in the system
     *
     * @return <tt>int</tt> number of non indexed BPs
     */
    public  final static int numNonIndexedBPs () {

        Connection conn = null;
        PreparedStatement pstmt = null;
        String msg = null;
        ResultSet rs = null;
        int numNotIndexed = 0;
        String sql  =null;

        try {
           conn  =  Conn.getConnection();
           sql  =JDBCService.getNamedSQL(conn,"wfm_numNonIndexedBPs");
           pstmt  = conn.prepareStatement(sql);
           pstmt.setInt(1,PlatformConstants.UNINDEXED_COMPUTE_FLAG);
           rs = pstmt.executeQuery();

            if (rs != null && rs.next()) {
               numNotIndexed = rs.getInt(1);
            }
        } catch(SQLException sqe) {
            msg = "WorkFlowMonitor.numNonIndexedBPs() caught SQLException.";
/*            WFGlobals.out.logException(msg, sqe);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFlowMonitor_numNonIndexedBPs", sqe);
            sqe.printStackTrace();
        } finally {
            try {
                if (rs != null)
                    rs.close();
                if (pstmt != null)
                    pstmt.close();
                if (conn != null) //created local must clean up
                    Conn.freeConnection(conn);

            } catch (SQLException sqe) {

                msg = "WorkFlowMonitor.numNonIndexedBPs() caught SQLException "+
                    "while trying to close prepared statement.";
/*                WFGlobals.out.logException(msg, sqe);*/
                WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFlowMonitor_numNonIndexedBPs1", sqe);
                sqe.printStackTrace();
            }
        }
        if(WFGlobals.out.debug) {
/*           WFGlobals.out.logDebug("[WorkFlowMonitor].numNonIndexedBPs num = " + numNotIndexed);*/
           WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "DEB_WorkFlowMonitor_numNonIndexedBPs" ,  new Object[]{ "" + numNotIndexed });
        }

        return numNotIndexed;
    }

    /**
     * Locate the list of all Branch Ids for a suggested Workflow
     * instance ID.
     *
     * @param workflowId - the workflowId to obtain the list of branchIds
     * @return <tt>Arraylist</tt> of Branch Ids that match suggested
     * Workflow instance ID. <tt>null</tt> if no matches are located.
     */
    public ArrayList getAllBranchIds (long workflowId, Connection c) {

        Connection conn = null;
        PreparedStatement pstmt = null;
        String msg = null;
        ResultSet rs=null;

        ArrayList list = new ArrayList();
        String sql = null;
        try {

            if (c == null)
                conn  =  Conn.getConnection();
            else
                conn = c;

            sql=JDBCService.getNamedSQL(conn,"wfm_getAllBranchIds");
            pstmt = conn.prepareStatement(sql);
            pstmt.setLong(1, workflowId);
            rs = pstmt.executeQuery();
            if (rs != null ) {
                  while (rs.next()) {
                     list.add(rs.getString("BRANCH_ID"));
                     }
             }  else {
/*                   WFGlobals.out.logError("No branch id found in workflow_context table" );*/
                   WFGlobals.out.logError( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_workflow_context");
                   }


           // list = this.getAllBranchIds(workflowId, pstmt);

        }  catch(SQLException sqe) {

            msg = myClassName + ".getAllBranchIds() caught SQLException.";
/*            WFGlobals.out.logException(msg, sqe);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getAllBranchIds" ,  new Object[]{ "" + myClassName }, sqe);
            sqe.printStackTrace();

        }  catch (Exception e) {

            msg = myClassName + ".getAllBranchIds() caught Exception.";
/*            WFGlobals.out.logException(msg, e);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getAllBranchIds1" ,  new Object[]{ "" + myClassName }, e);
            e.printStackTrace();
        } finally {
            try {
                if (rs != null)
                    rs.close();
                if (pstmt != null)
                    pstmt.close();

                if (conn != null && c == null) //created local must clean up
                    Conn.freeConnection(conn);

            } catch (SQLException sqe) {

                msg = myClassName + ".getAllBranchIds() caught SQLException "+
                    "while trying to close prepared statement.";
/*                WFGlobals.out.logException(msg, sqe);*/
                WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getAllBranchIds2" ,  new Object[]{ "" + myClassName }, sqe);
                sqe.printStackTrace();
            }
        }

        return list;
    }



    /**
     * Locate the list of all Branch Ids for a suggested Workflow
     * instance ID.
     *
     * @param workflowId - the workflowId to obtain the list of branchIds
     * @param pstmt - JDBC <tt>PreparedStatement</tt> to use.
     * @return <tt>Arraylist</tt> of Branch Ids that match suggested
     * Workflow instance ID. <tt>null</tt> if no matches are located.
     */
    ArrayList getAllBranchIds (long workflowId,
                               PreparedStatement pstmt) {

        ResultSet rs = null;

        ArrayList list = null;

        String msg = null;
        String id;

        try {
            pstmt.setLong(1, workflowId);
            rs = pstmt.executeQuery();

            if (rs != null) {
                list = new ArrayList();

                while (rs.next()) {

                    id = rs.getString("BRANCH_ID");

                    // if (WFGlobals.out.debug) {
                    //                    WFGlobals.out.logDebug(myClassName+
                    //                     "getAllBranchIds BRANCH_ID = '"+id
                    //                     +"'");
                    // }
                    list.add(id);
                }
            }
        } catch(SQLException sqe) {

            msg = myClassName + ".getAllIds() caught SQLException.";
/*            WFGlobals.out.logException(msg, sqe);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_SQLException233" ,  new Object[]{ "" + myClassName }, sqe);
            sqe.printStackTrace();

        } catch (Exception e) {

            msg = myClassName + ".getAllIds(conn) caught Exception.";
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getAllIds_conn5" ,  new Object[]{ "" + myClassName }, e);
            e.printStackTrace();
        } finally {
            try {
                if (rs != null)
                    rs.close();
            } catch (SQLException sqe) {

                msg = myClassName + ".getAllIds(conn) caught SQLException "+
                    "while trying to close prepared statement.";
                WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getAllIds_conn11" ,  new Object[]{ "" + myClassName }, sqe);
                sqe.printStackTrace();
            }
        }


        return list;
    }


    /**
     * Locate the list of all WFC Ids for a suggested Workflow
     * instance ID.
     *
     * @param workflowId - the workflowId to obtain the list of branchIds
     * @return <tt>HashMap</tt> of WFC Ids(key) and the step id
     * (value) that match suggested Workflow instance
     * ID. <tt>null</tt> if no matches are located.
     */
    public HashMap  getAllContextIds (long workflowId, Connection c) {

        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        String msg = null;
        String id = null;
        String stepId = null;
        // String branchId = null;

        HashMap map = null;
        String sql =null;

        try {

            if (c == null)
                conn  =  Conn.getConnection();
            else
                conn = c;

            sql=JDBCService.getNamedSQL(conn,"wfm_getAllContextIds");
            pstmt = conn.prepareStatement(sql);

            pstmt.setLong(1, workflowId);
            rs = pstmt.executeQuery();

            if (rs != null) {
                map = new HashMap();

                while (rs.next()) {

                    id = rs.getString("WFC_ID");
                    stepId = rs.getString("STEP_ID");
                    //branchId = rs.getString("BRANCH_ID");

                    // if (WFGlobals.out.debug) {
//                         WFGlobals.out.logDebug(myClassName+
//                                         "getAllBranchIds BRABCH_ID = '"+id
//                                         +"'");
                    // }
                    map.put(id, stepId);
                    // map.put(id+"-STEP", stepId);
//                  map.put(id+"-BRANCH", branchId);
                }
            }

        }  catch(SQLException sqe) {

            msg = myClassName + ".getAllBranchIds() caught SQLException.";
/*            WFGlobals.out.logException(msg, sqe);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getAllBranchIds3" ,  new Object[]{ "" + myClassName }, sqe);
            sqe.printStackTrace();

        }  catch (Exception e) {

            msg = myClassName + ".getAllBranchIds() caught Exception.";
/*            WFGlobals.out.logException(msg, e);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getAllBranchIds11" ,  new Object[]{ "" + myClassName }, e);
            e.printStackTrace();
        } finally {
            try {
                if (rs != null)
                    rs.close();
                if (pstmt != null)
                    pstmt.close();

                if (conn != null && c == null) //created local must clean up
                    Conn.freeConnection(conn);

            } catch (SQLException sqe) {

                msg = myClassName + ".getAllBranchIds() caught SQLException "+
                    "while trying to close prepared statement.";
/*                WFGlobals.out.logException(msg, sqe);*/
                WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getAllBranchIds21" ,  new Object[]{ "" + myClassName }, sqe);
                sqe.printStackTrace();
            }
        }

        return map;
    }



    /**
     * Locate the status of last row past the suggeted WFC Ids.
     *
     * @param wfcId - the workflow context Id to obtain the status of
     * row beyond.
     * @return <tt>HashMap</tt> for the last WFC Ids(key) past the
     * suggested WFC. It will contain the keys "BASIC_STATUS" and
     * "ADV_STATUS" that contain <tt>String</tt>
     * values. <tt>null</tt> if no after the suggested row.
     */
    public HashMap getNextContextStatus (String wfcId, Connection c) throws SQLException,Exception{
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String NEXTWFC="WorkFlowMonitorGetNextWfcStatus";

        String msg = null;
        String basicStatus = null;
        String advancedStatus = null;
        HashMap map = null;
        String sql=null;
        try {
            if (c == null)
                conn  =  Conn.getConnection();
            else
                conn = c;
            sql=JDBCService.getNamedSQL(conn, NEXTWFC);
            if (sql == null) {
                 WFGlobals.out.logError( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFlowMonitorgetnextWFCStatus");
           }

            pstmt = conn.prepareStatement(sql);

            pstmt.setString(1, wfcId);
            rs = pstmt.executeQuery();
            if (rs != null && rs.next()) {
                map = new HashMap();
                basicStatus    = rs.getString("BASIC_STATUS");
                advancedStatus = rs.getString("ADV_STATUS");
                map.put("BASIC_STATUS", basicStatus);
                map.put("ADV_STATUS", advancedStatus);
                }
        }  catch(SQLException sqe) {
            msg = myClassName + ".getNextContextStatus() caught SQLException.";
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getNextContextStatus" ,  new Object[]{ "" + myClassName }, sqe);
            sqe.printStackTrace();
            throw sqe;

        }  catch (Exception e) {

            msg = myClassName + ".getNextContextStatus() caught Exception.";
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getNextContextStatus1" ,  new Object[]{ "" + myClassName }, e);
            e.printStackTrace();
            throw e;
        } finally {
            try {
                if (rs != null)
                    rs.close();
                if (pstmt != null)
                    pstmt.close();
                if (conn != null && c == null) //created local must clean up
                    Conn.freeConnection(conn);
            } catch (SQLException sqe) {

                msg = myClassName + ".getNextContextStatus() caught SQLException "+
                    "while trying to close prepared statement.";
                WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getNextContextStatus2" ,  new Object[]{ "" + myClassName }, sqe);
                sqe.printStackTrace();
            }
        }
        return map;
    }




    /**
     * Tests whether the suggested Workflow instance is complete. The
     * definition of completed Workflow instance is all branches of a
     * Workflow have successfully completed.
     *
     * @param workflowId - the Workflow instance to test.
     * @return true if this workflow instance is complete, false
     * otherwise
     */
    public boolean isComplete(long workflowId) {
        return false;
    }

    /**
     *
     */
/*
    private boolean isComplete(long workflowId, PreparedStatement pstmt) {
        return false;
    }
*/
    public String getLastWFCId(long workflowId, String bid) {
        return getLastWFCId(workflowId, bid, null);
    }


    private String getLastWFCId(long workflowId, String bid, Connection c) {
        String sql =null;

        Connection conn = null;
        PreparedStatement pstmt = null;
        String msg = null;
        String WFCId = null;

        ResultSet rs = null;

        try {
            if (c == null) {
                conn  =  Conn.getConnection();
            } else {
                conn = c;
            }

            sql=JDBCService.getNamedSQL(conn,"wfm_getLastWFCId");
            pstmt = conn.prepareStatement(sql);
            pstmt.setMaxRows(1);

            pstmt.setLong(1, workflowId);
            pstmt.setString(2, bid);
            rs = pstmt.executeQuery();

            if ((rs != null) && rs.next()) {

                WFCId = rs.getString("WFC_ID");
            }
        }  catch(SQLException sqe) {

            msg = myClassName + ".getLastWFCId() caught SQLException.";
/*            WFGlobals.out.logException(msg, sqe);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getLastWFCId" ,  new Object[]{ "" + myClassName }, sqe);
            sqe.printStackTrace();

        }  catch (Exception e) {

            msg = myClassName + ".getLastWFCId() caught Exception.";
/*            WFGlobals.out.logException(msg, e);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getLastWFCId1" ,  new Object[]{ "" + myClassName }, e);
            e.printStackTrace();
        } finally {
            try {
                if (rs != null)
                    rs.close();

                if (pstmt != null)
                    pstmt.close();

                if (c == null && conn != null )
                    Conn.freeConnection(conn);

            } catch (SQLException sqe) {

                msg = myClassName +
                    ".getLastWFCId() caught SQLException "+
                    "while trying to close prepared statement.";
/*                WFGlobals.out.logException(msg, sqe);*/
                WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getLastWFCId2" ,  new Object[]{ "" + myClassName }, sqe);
                sqe.printStackTrace();
            }
        }

        return WFCId;
    }


    /**
     * Marks the suggested Workflow instance with a "Terminated" state.
     *
     * @param workflowId - the Workflow instance to test.
     * @return true if this workflow instance is updated, false
     * otherwise
     */
    public boolean terminateWF (String workflowId) {
        return mark(workflowId, TERMINATED, null,null);
    }
    public boolean terminateWF (String workflowId, WorkFlowContext wfc) {
        return mark(workflowId, TERMINATED, null, wfc);
    }

    public boolean terminateWF (String workflowId, boolean transactionOff,String user_id) throws SQLException{
        if (WFEvent.getEventFlag()) {
            this.user_id=user_id;
             }
        return terminateWF(workflowId, transactionOff);
    }

    public boolean terminateWF (String workflowId, boolean transactionOff)
        throws SQLException{
       Connection conn = null;
       try{
          if( transactionOff )
          {
             if( dbNoTransPool != null )
               conn = JDBCService.getConnection(dbNoTransPool);
             else
               //throw new SQLException(" WorkFlowMonitor: Unable to get connection from pool");
               throw new SQLException(WFGlobals.out.getlogMessageStr( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFlowMonitor_SQLException1_str1"));
          }

          return mark(workflowId, TERMINATED, conn,null);
         }
         catch(SQLException e)
         {

/*                WFGlobals.out.logException("[WFMonitor] terminateWF caught Exception", e);*/
                WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WFMonitor", e);
                throw e;
         }
         finally{
          if( conn != null )
              JDBCService.freeConnection( dbNoTransPool, conn);
         }
    }




    /**
     * Marks the suggested Workflow instance with a "Interrupted-Auto"
     * state. <br>
     *
     * Note it is the caller responsiblity to insure that the process
     * is no longer in an execution cycle.
     *
     * @param workflowId - the Workflow instance to test.
     * @return true if this workflow instance is updated, false
     * otherwise
     */
    public boolean interruptAutoWF (String workflowId) {

        // NOTE: Only manual interrupt currently supported.
        return mark(workflowId, INTERRUPTED_AUTO, null,null);
    }

     public boolean interruptAutoWF (String workflowId,String user_id) {

        if (WFEvent.getEventFlag()) {
            this.user_id=user_id;
             }
        // NOTE: Only manual interrupt currently supported.
        return mark(workflowId, INTERRUPTED_AUTO, null,null);
    }

    /**
     * Marks the suggested Workflow instance with a "Interrupted-Auto"
     * state. <br>
     *
     * Note it is the caller responsiblity to insure that the process
     * is no longer in an execution cycle.
     *
     * @param workflowId - the Workflow instance to test.
     * @param conn - a Connection to use for the transaction.
     * @return true if this workflow instance is updated, false
     * otherwise
     */
    public boolean interruptAutoWF (String workflowId, Connection conn,WorkFlowContext wfc) {

        // NOTE: Only manual interrupt currently supported.
        return mark(workflowId, INTERRUPTED_AUTO, conn,wfc);
    }

    public boolean interruptWF (String workflowId,String user_id) {
        if (WFEvent.getEventFlag()) {
            this.user_id=user_id;
             }

        // NOTE: Only manual interrupt currently supported.
        return mark(workflowId, INTERRUPTED_MAN, null,null);
        }


    /**
     * Marks the suggested Workflow instance with a "Interrupted"
     * state. <br>
     *
     * Note it is the caller responsiblity to insure that the process
     * is no longer in an execution cycle.
     *
     * @param workflowId - the Workflow instance to test.
     * @return true if this workflow instance is updated, false
     * otherwise
     */
    public boolean interruptWF (String workflowId) {

        // NOTE: Only manual interrupt currently supported.
        return mark(workflowId, INTERRUPTED_MAN, null,null);
    }


    /**
     * Marks the suggested Workflow instance with a "Interrupted"
     * state. <br>
     *
     * Note it is the caller responsiblity to insure that the process
     * is no longer in an execution cycle.
     *
     * @param workflowId - the Workflow instance to test.
     * @param conn - a Connection to use for the transaction.
     * @return true if this workflow instance is updated, false
     * otherwise
     */
    public boolean interruptWF (String workflowId, Connection conn,WorkFlowContext wfc) {

        // NOTE: Only manual interrupt currently supported.
        return mark(workflowId, INTERRUPTED_MAN, conn,wfc);
    }


    /**
     * Marks the suggested Workflow instance with a new state.
     *
     * @param workflowId - the Workflow instance to test.
     * @param state - the suggested state to mark the workflow instance with.
     * @return true if this workflow instance is updated, false
     * otherwise
     */
    protected boolean mark (String workflowId, int state, Connection conn, WorkFlowContext wfcIn) {

        WorkFlowContext wfc = null;
        boolean status = true;
        int wfState = getState(workflowId, conn);
        boolean bLock=false;
        SystemWorkFlowContext swfc = null;
        java.util.Date startTime = null;
        java.util.Date endTime = null;
        String WFCId = null;
        Vector WFCIds= new Vector();
	Event event = null;


        if (wfState == UNKNOWN)
            status = false;
        else {

            switch (state) {
            case TERMINATED:
                if (wfState != ACTIVE && wfState != TERMINATED &&
                    wfState != HALTING && wfState != COMPLETE) {
                    try {
                        startTime = new java.util.Date();
                        if (wfState == WAITING || wfState == WAITING_ON_IO){
                            stopWF(workflowId, startTime.getTime());
                            if(!terminatedLock) {
                                bLock=true;
			    }

                            if(!bLock){
                                if(WFGlobals.out.debug) {
                                    WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "DEB_WorkFlowMonitor_mark");
                                }
                                bLock= LockManager.lock("TERMINATE:"+workflowId,
                                                    "TERMINATE:"+workflowId ,
                                                    120000, 100000);

                                while(!bLock){
                                    bLock = LockManager.lock("TERMINATE:"+
                                                             workflowId,
                                                             "TERMINATE:"+
                                                             workflowId,
                                                             120000, 100000);
                                }
                            } else {
                                if (WFGlobals.out.debug) {
                                    WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "DEB_WorkFlowMonitor_mark1");
                                }
                            }
                            //need lock here to prevent the
                            //subworkflow to continue while it is
                            //terminated.
                            if (WFGlobals.out.debug) {
                                WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "DEB_TERMINATELOCK" ,  new Object[]{ "" + workflowId });
                            }
                            wfState=getState(workflowId,conn);
                        }

                        if (wfState != ACTIVE && wfState != TERMINATED &&
                            wfState != HALTING && wfState != COMPLETE) {

                         WFCIds=getLastWFCs(Long.parseLong(workflowId),state,conn);
                         if (WFCIds !=null && WFCIds.size()==0) {
                              status=false;
                              }
                         for (int i=0; i<WFCIds.size(); i++ ) {
                                WFCId=(String) WFCIds.elementAt(i);
                                wfc=read(conn, WFCId);
                                wfc.setStartTime(startTime.getTime());
                                wfc.setPrevWorkFlowContextId(wfc.getWorkFlowContextId());
                                String wfc_id=null;
                                wfc_id=wfc.getWorkFlowContextId();
                                wfc.setWorkFlowContextId(Util.createGUID());
                                wfc.setNextActivityInfoId(-1);
                                wfc.incrementStepId();

                                String originalServiceName=wfc.getServiceName();
                                wfc.setServiceName(DEFAULT_SERVICENAME);

                                // keep it simple for terminate -
                                // don't need an adv status here
                                wfc.setBasicStatus(WorkFlowContext.WF_TERMINATED);
                                wfc.setWFEBasicStatus(WorkFlowContext.WFE_INSTANCE_TERMINATED);
                                wfc.setAdvancedStatus(null);
                                if (wfcIn !=null ) {
                                    wfc.setWFStatusRpt("Status_Report",
                                      "Process terminated by wf "+ wfcIn.getWorkFlowId() + " from service " +wfcIn.getServiceName() );
                                } else {
                                    wfc.setWFStatusRpt("Status_Report",
                                     "Process terminated");
                                   }

                                if (WFGlobals.out.debug) {
                                    WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "DEB_myClassName2" ,  new Object[]{ "" + myClassName , "" + wfc });
                                }

                                endTime = new java.util.Date();
                                wfc.setEndTime(endTime.getTime());

                                //Since process data and the system
                                //context are not available at this time, this executes
                                //a SQL query to determine if the
                                //sequence information must be removed.
                                //This is less than ideal in terms of
                                //efficiency and should be looked into
                                SequencedLockManager.removeSequencedLockInformation(wfc, conn);

                                if( conn == null ) {
                                    wfc.persist(WFGlobals.PERSISTENCE_MINIMAL);
                                } else{
                                    wfc.persist(conn,WFGlobals.PERSISTENCE_MINIMAL);
                                }
                                if ( WFEvent.getEventFlag() ) {
                                     if (user_id !=null && !user_id.equals("")) {
                                        wfc.setWFStatusRpt("USER_ID",user_id);
                                        }
                                     }
                                WFEvent.fireAbnormalEvent("BPTerminated_MIN",wfc);
                                int prev_step=wfc.getStepId()-1;
                                if ( prev_step >0 && isConsumerService(originalServiceName)) {
                                    unregisterDeadConsumer(wfc_id,conn);
                                    WFEvent.fireNormalEvent("DeleteExpiredConsume_MIN",wfc);
                                    BPDeadLineUtil.updateDeadLineTable(wfc,DBDeadLineBase.TERMINATE);
                                }
				event = Event.findByWorkFlowContextId(wfc_id);
				if (event != null) {
				    if (event.remove(event)) {
					if (WFGlobals.out.debug) {
					    WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "DEB_WorkFlowMonitor_mark2" ,  new Object[]{ "" + event });
					}
                                        WFEvent.fireNormalEvent("RemoveEvent_MIN",wfc);
				    } else {
					WFGlobals.out.logError( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFlowMonitor_mark" ,  new Object[]{ "" + event });
                                        WFEvent.fireAbnormalEvent("RemoveEventError",wfc);
				    }
				    event = null;
				}
			 }
                        } else if (wfState == HALTING) {
                            WFCIds=getLastWFCs(Long.parseLong(workflowId),state,conn);
                            if (WFCIds !=null && WFCIds.size()==0) {
                              status=false;
                              }
                            for (int i=0; i<WFCIds.size(); i++ ) {
                                WFCId=(String) WFCIds.elementAt(i);
                                wfc=read(conn, WFCId);
                                if (wfc.getBasicStatus() == WorkFlowContext.WAITING || wfc.getBasicStatus() == WorkFlowContext.WAITING_ON_IO)  {
                                   wfc.setStartTime(startTime.getTime());
                                   wfc.setPrevWorkFlowContextId(wfc.getWorkFlowContextId());
                                   wfc.setWorkFlowContextId(Util.createGUID());
                                   wfc.setNextActivityInfoId(-1);
                                   wfc.incrementStepId();
                                   wfc.setServiceName(DEFAULT_SERVICENAME);
                                   // keep it simple for terminate -
                                   // don't need an adv status here
                                   wfc.setBasicStatus(WorkFlowContext.WF_TERMINATED);
                                   if (wfc.getWFEBasicStatus()!= WFCBase.WFE_WAITING_ON_IO) {
                                        wfc.setWFEBasicStatus(WorkFlowContext.WFE_INSTANCE_TERMINATED);
                                        }
                                   wfc.setAdvancedStatus(null);
                                   if (wfcIn !=null ) {
                                       wfc.setWFStatusRpt("Status_Report",
                                          "Process terminated by wf "+ wfcIn.getWorkFlowId() + " from service " +wfcIn.getServiceName() );
                                   } else {
                                       wfc.setWFStatusRpt("Status_Report",
                                           "Process terminated");
                                       }
                                   if (WFGlobals.out.debug) {
                                       WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "DEB_myClassName3" ,  new Object[]{ "" + myClassName , "" + wfc });
                                   }
                                   endTime = new java.util.Date();
                                   wfc.setEndTime(endTime.getTime());

                                   if( conn == null ) {
                                       wfc.persist(WFGlobals.PERSISTENCE_MINIMAL);
                                   } else{
                                       wfc.persist(conn,WFGlobals.PERSISTENCE_MINIMAL);
                                   }
                                    if ( WFEvent.getEventFlag() ) {
                                     if (user_id !=null && !user_id.equals("")) {
                                        wfc.setWFStatusRpt("USER_ID",user_id);
                                        }
                                     }

                                   WFEvent.fireAbnormalEvent("BPTerminated_MIN",wfc);
                                }
                            }
                        } else {
                            wfc.setBasicStatus(WorkFlowContext.ERROR);
                            wfc.setAdvancedStatus("Cannot terminate");
                            wfc.setWFStatusRpt("Status_Report",
                                            "Cannot terminate this workflow.");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        status = false;
                    } finally {
                        if (bLock &&
                            !LockManager.unlock("TERMINATE:"+workflowId)){
                            WFGlobals.out.logError( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_lock_TERMINATE" ,  new Object[]{ "" + workflowId });
                        }
                    }
                } else {
                    if (WFGlobals.out.debug) {
                        WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "DEB_myClassName4" ,  new Object[]{ "" + myClassName });
                    }
                    status = false;
                }

                break;
            case INTERRUPTED_MAN:
            case INTERRUPTED_AUTO:
                startTime = new java.util.Date();

                if (wfState != TERMINATED &&
                    wfState != COMPLETE &&
                    wfState != INTERRUPTED_MAN &&
                    wfState != INTERRUPTED_AUTO) {

                    try {
                      WFCIds=getLastWFCs(Long.parseLong(workflowId),state,conn);
                      for (int i=0; i<WFCIds.size();i++) {
                           wfc = new WorkFlowContext();
                           WFCId=(String) WFCIds.elementAt(i);
                           wfc=read(conn,WFCId);
                            wfc.setStartTime(startTime.getTime());
                            wfc.setPrevWorkFlowContextId(wfc.getWorkFlowContextId());
                            wfc.setWorkFlowContextId(Util.createGUID());
                            wfc.incrementStepId();
                            wfc.setAdvancedStatus(null);
                            wfc.setServiceName(DEFAULT_SERVICENAME);
                            if (state == INTERRUPTED_MAN) {
                               wfc.setBasicStatus(wfc.WF_INTERRUPT_MAN);
                            } else {
                               wfc.setBasicStatus(wfc.WF_INTERRUPT_AUTO);
                            }
                            wfc.setAdvancedStatus(wfc.WFC_ADVSTATUS_SERVICE_INTERRUPTED);
                            if (wfc.getWFEBasicStatus()!= WFCBase.WFE_WAITING_ON_IO) {
                               wfc.setWFEBasicStatus(wfc.WFE_SERVICE_INTERRUPTED);
                               }
                            if (wfcIn !=null) {
                                wfc.setWFStatusRpt("Status_Report",
                                               "The WF_id: " +workflowId +
                                               " is marked as Interrupted. by wf " + wfcIn.getWorkFlowId()+ " from service " +wfcIn.getServiceName());
                            } else {
                                wfc.setWFStatusRpt("Status_Report",
                                               "The WF_id: " +workflowId +
                                               " is marked as Interrupted.");
                                }
                            endTime = new java.util.Date();
                            wfc.setEndTime(endTime.getTime());
                            if (WFGlobals.out.debug) {
                                WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "DEB_myClassName5" ,  new Object[]{ "" + myClassName , "" + wfc });
                            }

                            if( conn == null ) {
                                wfc.persist(WFGlobals.PERSISTENCE_MINIMAL);
                            } else{
                                wfc.persist(conn, WFGlobals.PERSISTENCE_MINIMAL);
                            }
                            WFEvent.fireAbnormalEvent("BPInterrupted_MIN",wfc);
                       }
                    if (WFCIds!=null && WFCIds.size()==0) {
                        status = false;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        status = false;
                    }
                } else {
                    if (WFGlobals.out.debug) {
                        WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "DEB_INTERRUPTION" ,  new Object[]{ "" + myClassName });
                    }
                    status = false;
                }

                break;
            default:
                if (WFGlobals.out.debug) {
                    WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "DEB_myClassName6" ,  new Object[]{ "" + myClassName });
                }
                status = false;
                break;
            }
        }

        return status;
    }

    private Vector getLastWFCs(long workflowId, int state) {
       return getLastWFCs(workflowId, state, null);
    }
    private Vector getLastWFCs(long workflowId, int state, Connection conn) {

   Vector tmp = new Vector();
   ArrayList branchList = null;
   String bid;
   int bState=UNKNOWN;

   boolean isConnNull = false;
   //Connection conn=null;
   try {
      if ( conn == null ) {
        conn=Conn.getConnection();
        isConnNull=true;
      }
      branchList = getAllBranchIds(workflowId, conn);
      if (branchList !=null && branchList.size()>0) {
        for (Iterator it = branchList.iterator(); it.hasNext(); ) {
          bid = (String)it.next();
          bState = getBranchState(bid, conn, workflowId);
          if (state==INTERRUPTED_AUTO || state==INTERRUPTED_MAN) {
             if (bState == ACTIVE || bState==HALTING ||
                 bState==WAITING_ON_IO ) {
//               tmp.add(getLastWFCId(workflowId,bid));
               tmp.add(getLastWFCId(workflowId,bid,conn));
               }
          } else if( state==TERMINATED) {//terminate
             if ((bState == WAITING) ||
		 (bState == WAITING_ON_IO) ||
		 (bState==HALTED)||
		 (bState==INTERRUPTED_AUTO)||
		 (bState == INTERRUPTED_MAN)) {
//		 tmp.add(getLastWFCId(workflowId,bid));
                 tmp.add(getLastWFCId(workflowId,bid,conn));
	     }
          }  else {//force_terminated
          //   if (bState != COMPLETE && bState != ACTIVE_WAITING) {
//                  tmp.add(getLastWFCId(workflowId,bid));
                  tmp.add(getLastWFCId(workflowId,bid,conn));
           //       }
             }
          }
      } else {
          tmp=null;
          }
   }  catch (SQLException se) {
/*        WFGlobals.out.logException("WorkFlowMOnitor.getLastWFCs() caught sqlexception",se);*/
        WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFlowMOnitor_getLastWFCs",se);
   }  finally {
       //if (conn != null)
       if (isConnNull && conn != null) 
          Conn.freeConnection(conn);
     }
    return tmp;


   }


    public int getInstanceState (long wfId, Connection c) {

        int state = UNKNOWN;

        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        String msg = null;
        String sql=null;
        try {
            if (c == null) {
                conn = Conn.getConnection();
            } else {
                conn = c;
            }
            sql=JDBCService.getNamedSQL(conn,"wfm_getInstanceState");
            pstmt = conn.prepareStatement(sql);
            pstmt.setMaxRows(1);

            pstmt.setLong(1, wfId);
            rs = pstmt.executeQuery();

            if ((rs != null) && rs.next()) {
                state = rs.getInt("STATE");
            }
        }  catch(SQLException sqe) {
            msg = myClassName + ".getInstanceState() caught SQLException.";
/*            WFGlobals.out.logException(msg, sqe);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getInstanceState" ,  new Object[]{ "" + myClassName }, sqe);
            sqe.printStackTrace();
        }  catch (Exception e) {
            msg = myClassName + ".getInstanceState() caught Exception.";
/*            WFGlobals.out.logException(msg, e);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getInstanceState1" ,  new Object[]{ "" + myClassName }, e);
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }

                if (pstmt != null) {
                    pstmt.close();
                }

                if (conn != null && c == null) {
                    Conn.freeConnection(conn);
                }
            } catch (SQLException sqe) {
                msg = myClassName +
                    ".getInstanceState() caught SQLException "+
                    "while trying to close prepared statement.";
/*                WFGlobals.out.logException(msg, sqe);*/
                WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getInstanceState2" ,  new Object[]{ "" + myClassName }, sqe);
                sqe.printStackTrace();
            }
        }

        return state;
    }


    public int getInstanceStatus (long wfId, Connection c) {

        int status = UNKNOWN;

        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        String msg = null;
        String sql= null;

        try {

            if (c == null) {
                conn = Conn.getConnection();
            } else {
                conn = c;
            }
            sql=JDBCService.getNamedSQL(conn,"wfm_getInstanceStatus");
            pstmt = conn.prepareStatement(sql);
            pstmt.setMaxRows(1);

            pstmt.setLong(1, wfId);
            rs = pstmt.executeQuery();

            if ((rs != null) && rs.next()) {
                status = rs.getInt("STATUS");
            }
        }  catch(SQLException sqe) {
            msg = myClassName + ".getInstanceStatus() caught SQLException.";
/*            WFGlobals.out.logException(msg, sqe);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getInstanceStatus" ,  new Object[]{ "" + myClassName }, sqe);
            sqe.printStackTrace();
        }  catch (Exception e) {
            msg = myClassName + ".getInstanceStatus() caught Exception.";
/*            WFGlobals.out.logException(msg, e);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getInstanceStatus1" ,  new Object[]{ "" + myClassName }, e);
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }

                if (pstmt != null) {
                    pstmt.close();
                }

                if (conn != null && c == null) {
                    Conn.freeConnection(conn);
                }
            } catch (SQLException sqe) {
                msg = myClassName +
                    ".getInstanceStatus() caught SQLException "+
                    "while trying to close prepared statement.";
/*                WFGlobals.out.logException(msg, sqe);*/
                WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getInstanceStatus2" ,  new Object[]{ "" + myClassName }, sqe);
                sqe.printStackTrace();
            }
        }

        return status;
    }

/*
         //get service of wf
    public String getService(long workflowId, Connection c){

        String service = null;
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = "SELECT SERVICE_NAME FROM WORKFLOW_CONTEXT "+
            "WHERE WORKFLOW_ID=?";

        try {
            if (c == null) {
                conn  =  Conn.getConnection();
            } else {
                conn = c;
            }
            pstmt = conn.prepareStatement(sql);
            pstmt.setLong(1, workflowId);
            rs = pstmt.executeQuery();
            //TODO: sort list
            if (rs != null) {
                while (rs.next()) {
                    service = rs.getString(1);
                }
            }
        }catch(SQLException sqle){
            WFGlobals.out.logException("[WorkFlowMonitor].getService()", sqle);
        } finally {
            try {
                if (rs != null)
                    rs.close();

                if (pstmt != null)
                    pstmt.close();

                if (conn != null )
                    Conn.freeConnection(conn);

            } catch (SQLException sqe) {
                WFGlobals.out.logException("WorkFlowMonitor].getService() closing", sqe);
            }
        }


        return service;

    }

    public String getService(long workflowId, String branchId, Connection c){

        String service = null;
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = "SELECT SERVICE_NAME FROM WORKFLOW_CONTEXT "+
            "WHERE WORKFLOW_ID=? and BRANCH_ID=?";

        try {
            if (c == null) {
                conn  =  Conn.getConnection();
            } else {
                conn = c;
            }
            pstmt = conn.prepareStatement(sql);
            pstmt.setLong(1, workflowId);
            pstmt.setString(2, branchId);
            rs = pstmt.executeQuery();

            // TODO: sort list
            if (rs != null) {
                while (rs.next()) {
                    service = rs.getString(1);
                }
            }
        }catch(SQLException sqle){
            WFGlobals.out.logException("[WorkFlowMonitor].getService()", sqle);
        } finally {
            try {
                if (rs != null)
                    rs.close();

                if (pstmt != null)
                    pstmt.close();

                if (conn != null )
                    Conn.freeConnection(conn);

            } catch (SQLException sqe) {
                WFGlobals.out.logException("[WorkFlowMonitor].getService() closing", sqe);
            }
        }

        return service;
    }
*/
    public int getPersistenceLevel(int workflowId, Connection c){
        int level = -1;
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = null;

        try {
            if (c == null) {
                conn  =  Conn.getConnection();
            } else {
                conn = c;
            }
            sql=JDBCService.getNamedSQL(conn,"wfm_getPersistenceLevel");
            pstmt = conn.prepareStatement(sql);
            pstmt.setLong(1, workflowId);
            rs = pstmt.executeQuery();

            if (rs != null) {
                while (rs.next()) {
                    level = rs.getInt(1);
                }
            }
        }catch(SQLException sqle){
/*            WFGlobals.out.logException("[WorkFlowMonitor].getPersistenceLevel()", sqle);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFlowMonitor_getPersistenceLevel", sqle);
        } finally {
            try {
                if (rs != null)
                    rs.close();

                if (pstmt != null)
                    pstmt.close();

                if (conn != null )
                    Conn.freeConnection(conn);

            } catch (SQLException sqe) {
/*                WFGlobals.out.logException("[WorkFlowMonitor].getPersistenceLevel() closing", sqe);*/
                WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFlowMonitor_getPersistenceLevel1", sqe);
            }
        }

        return level;
    }

    public int getPersistenceLevel(int workflowId, String branchId, Connection c){
        int level = -1;
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql =null;

        try {
            if (c == null) {
                conn  =  Conn.getConnection();
            } else {
                conn = c;
            }
            sql=JDBCService.getNamedSQL(conn,"wfm_getPersistenceLevel_s1");
            pstmt = conn.prepareStatement(sql);
            pstmt.setLong(1, workflowId);
            pstmt.setString(2, branchId);

            rs = pstmt.executeQuery();

            if (rs != null) {
                while (rs.next()) {
                    level = rs.getInt(1);
                }
            }
        }catch(SQLException sqle){
/*            WFGlobals.out.logException("[WorkFlowMonitor].getPersistenceLevel()", sqle);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFlowMonitor_getPersistenceLevel2", sqle);
        } finally {
            try {
                if (rs != null)
                    rs.close();

                if (pstmt != null)
                    pstmt.close();

                if (conn != null )
                    Conn.freeConnection(conn);

            } catch (SQLException sqe){
/*                WFGlobals.out.logException("[WorkFlowMonitor].getPersistenceLevel() closing", sqe);*/
                WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFlowMonitor_getPersistenceLevel11", sqe);
            }
        }
        return level;
    }

    public int getPersistenceLevel(int workflowId, String branchId, int stepId, Connection c){
        int level = -1;
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = null;

        try {
            if (c == null) {
                conn  =  Conn.getConnection();
            } else {
                conn = c;
            }
            sql=JDBCService.getNamedSQL(conn,"wfm_getPersistenceLevel_s2");
            pstmt = conn.prepareStatement(sql);
            pstmt.setLong(1, workflowId);
            pstmt.setString(2, branchId);
            pstmt.setInt(3, stepId);

            rs = pstmt.executeQuery();
            // TODO: should not have to sort, but ..
            if (rs != null) {
                while (rs.next()) {
                    level = rs.getInt(1);
                }
            }
        }catch(SQLException sqle){
/*            WFGlobals.out.logException("[WorkFlowMonitor].getPersistenceLevel()", sqle);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFlowMonitor_getPersistenceLevel3", sqle);
        } finally {
            try {
                if (rs != null)
                    rs.close();

                if (pstmt != null)
                    pstmt.close();

                if (conn != null )
                    Conn.freeConnection(conn);

            } catch (SQLException sqe){
/*                WFGlobals.out.logException("[WorkFlowMonitor].getPersistenceLevel() closing", sqe);*/
                WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFlowMonitor_getPersistenceLevel12", sqe);
            }
        }
        return level;
    }




    public int getLastValidBranchStepId(long workflowId, String branchId, int[] anyState){

        int step = -2;
        int tState = -2;
        int nStates = anyState.length;
        Connection conn = null;
        ResultSet rs = null;
        PreparedStatement pstmt = null;
        /// get all steps in this branch, start at last step looking for a step which has anyState
        String sql =null;

        try {
            conn  =  Conn.getConnection();
            sql =JDBCService.getNamedSQL(conn,"wfm_getLastValidBranchStepId");
            pstmt = conn.prepareStatement(sql);
            pstmt.setLong(1, workflowId);
            pstmt.setString(2, branchId);

            rs = pstmt.executeQuery();

            if (rs != null) {
                while (rs.next()) {
                    tState = rs.getInt(1);
                    for(int i=0;i<nStates;i++){
                        if(tState==anyState[i]){
                            step = rs.getInt(2);
                            return step;
                        }
                    }
                }

            }
        }catch(SQLException sqle){
/*            WFGlobals.out.logException("[WorkFlowMonitor].getLastValidBranchStepId()", sqle);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFlowMonitor_getLastValidBranchStepId", sqle);
        } finally {
            try {
                if (rs != null)
                    rs.close();

                if (pstmt != null)
                    pstmt.close();

                if (conn != null )
                    Conn.freeConnection(conn);

            } catch (SQLException sqe){
/*                WFGlobals.out.logException("[WorkFlowMonitor].getLastValidBranchStepId() closing", sqe);*/
                WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFlowMonitor_getLastValidBranchStepId1", sqe);
            }
        }

        return step;
    }

/*
    private String decimalToBinary(int number) {
        int digit = Integer.MIN_VALUE;
        char[] binary = new char[32];

        for(int counter=0;counter<32;counter++) {
            if ((number & digit) == 0) binary[31-counter]=0;
            else binary[31-counter]=1;
            digit = digit>>>1;
        }

        return new String(binary);
    }
*/

    private int getNextAIId(String wfcId, Connection c){
        int nextAIId = -1;
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql =null;
        try {
            if (c == null) {
                conn  =  Conn.getConnection();
            } else {
                conn = c;
            }
            sql=JDBCService.getNamedSQL(conn,"wfm_getNextAIId");
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, wfcId);

            rs = pstmt.executeQuery();

            if (rs != null) {
                while (rs.next()) {
                    nextAIId = rs.getInt(1);
                }
            }
        } catch(SQLException sqle){
/*            WFGlobals.out.logException("[WorkFlowMonitor].getNextAIId()",sqle);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFlowMonitor_getNextAIId",sqle);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }

                if (pstmt != null) {
                    pstmt.close();
                }

                if (c == null && conn != null ) {
                    Conn.freeConnection(conn);
                }
            } catch (SQLException sqe){
/*                WFGlobals.out.logException("[WorkFlowMonitor].getPersistenceLevel() closing", sqe);*/
                WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFlowMonitor_getPersistenceLevel13", sqe);
            }
        }
        return nextAIId;
    }


    public int getRecoveryLevel(long argId) {

      int level = BPRecoveryProperties.UNKNOWN;
      String key = null;
      WorkFlowDef wfd = null;

      Connection conn = null;
      PreparedStatement pstmt = null;
      PreparedStatement pstmt1 = null;
      ResultSet rs = null;

      try {

         conn = Conn.getConnection();
         String sqltxt=JDBCService.getNamedSQL(conn,"wfd_loadRecoveryLevel");
         pstmt = conn.prepareStatement(sqltxt);
         pstmt.setLong(1,argId);
         rs=pstmt.executeQuery();


         if(rs.next()){
            level = rs.getInt(1);

         } else {
            level = BPRecoveryProperties.UNKNOWN;
        }
      } catch (SQLException  sqe) {
         WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "wfd_loadRecoveryLevel",sqe);
      } finally {
         try {
           if (rs != null) { rs.close(); }
           if (pstmt != null) { pstmt.close(); }
           Conn.freeConnection(conn);
         } catch (Exception e) {}
      }

        if (level == BPRecoveryProperties.UNKNOWN && key != null && bpRecoveryProps.containsKey(key)) { // properties file
            level = bpRecoveryProps.get(key);
            if (WFGlobals.out.debug) {
/*                WFGlobals.out.logDebug("[WorkFlowMonitor].getRecoveryLevel(), BP KEY="+key+" BPRecoveryProperties file.recoveryLevel="+level);*/
                WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "DEB_WorkFlowMonitor_getRecoveryLevel1" ,  new Object[]{ "" + key , "" + level });
            }
        }

        if (level == BPRecoveryProperties.UNKNOWN) { //not in map try AI
            ArrayList list = getAllNextServices(argId, wfd);
            ServiceMetaData sdi = null;
            int persistenceLevel = -1;

            if (list!=null && list.size()>0) {
              for (Iterator it = list.iterator(); it.hasNext();){
                sdi = (ServiceMetaData)it.next();
                persistenceLevel = sdi.getPersistenceLevel();

                if (WFGlobals.out.debug) {
/*                    WFGlobals.out.logDebug("[WorkFlowMonitor].getRecoveryLevel(), sdi.getPersistenceLevel() = "+
                                       persistenceLevel);*/
                    WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "DEB_WorkFlowMonitor_getRecoveryLevel2" ,  new Object[]{ "" + persistenceLevel });
                    }

                if (persistenceLevel == WFGlobals.PERSISTENCE_NONE){
                    level = BPRecoveryProperties.AUTO_RESUME;
                    }
                }
              }
        }

        if (level == BPRecoveryProperties.UNKNOWN) { //default
            level = BPRecoveryProperties.MANUAL;
        }

        return level;
    }




    private  ArrayList getAllNextServices(long wfId, WorkFlowDef wfd) {
        if(wfd==null)return null;
        ArrayList branchIdList = null;
        String branchId = null;
        Connection conn = null;
        ActivityInfo ai = null;
        String nextServiceName = null;
        String wfcId = null;
        int nextAIId = -1;
        ArrayList list = null;
        ServiceMetaData sdi = null;

        if (wfd != null) {
            list = new ArrayList(5);

            try {

            conn  =  Conn.getConnection();
            branchIdList = getAllBranchIds(wfId, conn);

            for (Iterator it = branchIdList.iterator(); it.hasNext();){
                branchId = (String)it.next();
                wfcId = getLastWFCId(wfId, branchId, conn);
                if(wfcId!=null) {
                    nextAIId = getNextAIId(wfcId, conn);
                    if(nextAIId!=-1){
                        ai = wfd.getActivity(nextAIId);
                        if(ai!=null){
                            nextServiceName = ai.getServiceName();
                           IComponentExecutor compExecutor = IComponentExecutorFactory.getInstance( WEHelper.getDefaultModeClass() );
                            sdi = new ServiceMetaData(nextServiceName, compExecutor);

                            if(sdi != null){
                                list.add(sdi);
                                if (WFGlobals.out.debug) {
/*                                    WFGlobals.out.logDebug("[WorkFlowMonitor].getAllNextServices(), add "+nextServiceName+" is to list");*/
                                    WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "DEB_WorkFlowMonitor_getAllNextServices" ,  new Object[]{ "" + nextServiceName });
                                }
                            } else {
/*                                WFGlobals.out.logError("[WorkFlowMonitor].getAllNextServices(), NOT a valid service = "+nextServiceName);*/
                                WFGlobals.out.logError( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFlowMonitor_getAllNextServices" ,  new Object[]{ "" + nextServiceName });
                            }
                        } else {
/*                            WFGlobals.out.logError("[WorkFlowMonitor].getAllNextServices(), NO valid activity for nextAIId = "+nextAIId);*/
                            WFGlobals.out.logError( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFlowMonitor_getAllNextServices1" ,  new Object[]{ "" + nextAIId });
                        }
                    } else {
/*                        WFGlobals.out.logError("[WorkFlowMonitor].getAllNextServices(), NO valid NEXT Activity for wfcId = "+wfcId );*/
                        WFGlobals.out.logError( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFlowMonitor_getAllNextServices2" ,  new Object[]{ "" + wfcId });
                    }
                } else {
/*                    WFGlobals.out.logError("[WorkFlowMonitor].getAllNextServices(), NO valid Last WFCId for workflowId = "+wfId+" and branch ID= "+branchId);*/
                    WFGlobals.out.logError( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFlowMonitor_getAllNextServices3" ,  new Object[]{ "" + wfId , "" + branchId });
                }
            }
            } catch(SQLException sqle){
/*            WFGlobals.out.logException("[WorkFlowMonitor].getAllNextServices()",
                                       sqle);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFlowMonitor_getAllNextServices4",                                       sqle);
            } finally {
                if (conn != null ) {
                    Conn.freeConnection(conn);
                }
            }
        }

        return list;
    }

    /**
     * method to simply terminate a workflow
     *
     *
     */
    public boolean terminateWorkFlowSimple(long workflowId, long newWorkflowId, Connection conn) {

        String WFCId = null;
        java.util.Date startTime = new java.util.Date();
        WorkFlowContext wfc = null;
        Vector WFCIds = getLastWFCs(workflowId,TERMINATED);
        // SystemWorkFlowContext swfc = null;
        java.util.Date endTime = null;
        int lSize = -1;
        if(WFCIds!=null)lSize = WFCIds.size();
        if (WFGlobals.out.debug) {
            WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "DEB_WorkFlowMonitor_terminateWorkFlowSimple" ,  new Object[]{ "" + lSize });
        }
        try {
            for (Iterator it = WFCIds.iterator(); it.hasNext(); ) {

                WFCId=(String)it.next();
                if (WFGlobals.out.debug) {
                    WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "DEB_WorkFlowMonitor_terminateWorkFlowSimple1" ,  new Object[]{ "" + WFCId });
                }
                // this method re-initializes the wfc object each time throught the loop
                wfc=read(conn,WFCId);
                // terminate just copies the wfc and puts a new status on the
                // new wfc.  If we are terminating a waiting Invoke service
                // we don't want to copy the IWF_Id to the next row, so
                // delete it here don't need for simple read
                //swfc = new SystemWorkFlowContext(wfc);
                //swfc.remove("IWF_Id");
                //swfc.remove("INITIALCONTEXT_LIST");

                wfc.setStartTime(startTime.getTime());
                wfc.setPrevWorkFlowContextId(wfc.getWorkFlowContextId());
                wfc.setWorkFlowContextId(Util.createGUID());
                wfc.setNextActivityInfoId(-1);
                wfc.incrementStepId();
                String originalServiceName=wfc.getServiceName();
                wfc.setServiceName("BUSINESS_PROCESS_TERMINATE");

                // keep it simple for terminate - don't need an adv status here
                wfc.setBasicStatus(WorkFlowContext.WF_TERMINATED);
                wfc.setWFEBasicStatus(WorkFlowContext.WFE_INSTANCE_TERMINATED);
                wfc.setAdvancedStatus(null);
                wfc.setWFStatusRpt("Status_Report","Process terminated, Restarted with new WorkFlow Id = "+newWorkflowId);
                endTime = new java.util.Date();
                wfc.setEndTime(endTime.getTime());
                if( conn == null ) {// not called within transaction use default
                    wfc.persist(WFGlobals.PERSISTENCE_MINIMAL);
                } else{
                    wfc.persist(conn,WFGlobals.PERSISTENCE_MINIMAL);
                }
                int prev_step=wfc.getStepId()-1;
                if ( prev_step>0 && isConsumerService(originalServiceName)) {
                  unregisterDeadConsumer(WFCId,conn);
                  }
            }
            if (WFGlobals.out.debug) {
                WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "DEB_WorkFlowMonitor_terminateWorkFlowSimple2" ,  new Object[]{ "" + workflowId });
            }
        } catch(SQLException sqle){
/*            WFGlobals.out.logException("[WorkFlowMonitor].terminateWorkFlowSimple() dB error", sqle);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFlowMonitor_terminateWorkFlowSimple", sqle);
            return false;
        }
        return true;
    }


/* it is called from terminate action, to find out
 * if this is consumer service in waiting.
 *
 */

    private boolean  isConsumerService(String serviceName) {

       ServicesControllerImpl sci = ServicesControllerImpl.getInstance();
       ServiceInfo si = null;
       String serviceDefName=null;
       SII sii = null;

       if (serviceName != null && (
           serviceName.equals(DEFAULT_SERVICENAME)||
           serviceName.indexOf("INITIATING_CONTEXT")>-1 ||
           serviceName.equals("System_Service"))){
           return false;
       }
       if (sci != null) {
           // try the cache
           si = sci.getServiceInfo( serviceName );
       } else {
           // if not fetch from the db
           si = new ServiceInfo(serviceName);
           si.load();
       }

       if (si != null ) {
            sii = si.getSII();
       }

       if (sii != null) {
           SDI sdi=sii.getSDI();
           if (sdi != null) {
               serviceDefName=sdi.getName();
               if (WFGlobals.out.debug) {
                   WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "DEB_WorkFlowMOnitor" ,  new Object[]{ "" + serviceName , "" + serviceDefName });
               }
           }
       }  else {
             WFGlobals.out.logError( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFlowMOnitor" ,  new Object[]{ "" + serviceName , "" + serviceName });

       }

        if (serviceDefName==null || serviceDefName.equals("")) {
            serviceDefName=serviceName;
        }

       if (serviceDefName!= null && serviceDefName.toUpperCase().indexOf("CONSUME") >-1) {
             return true;
        } else {
             return false;
             }
       }

/*
 * if wf is terminated, all the waiting dead consumer should be unregistered
 */
   private void unregisterDeadConsumer(String wfc_id, Connection conn)throws SQLException {
        WorkFlowMessageConsumer wfmc=new WorkFlowMessageConsumer();
        wfmc.unregisterConsumer(wfc_id,conn);
   }

    public boolean forceTerminateWF(String wfIDStr) {
       return(forceTerminateWF(wfIDStr, null));
    }

    public boolean forceTerminateWF(String wfIDStr, Connection conn) {
        long wfId = 0;
        boolean success = false;

        try {
            wfId = Long.parseLong(wfIDStr);
            success = forceTerminateWF(wfId, conn);
        } catch (NumberFormatException ne) {
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFLowMonitor_forceTermiateWF" ,  new Object[]{ "" + wfIDStr }, ne );
        }

        return success;
    }
    public boolean forceTerminateWF(long wf_id) {
       return(forceTerminateWF(wf_id, null)); 
    }

    public boolean forceTerminateWF(long wf_id, Connection conn) {
          Vector WFCIds=null;
          java.util.Date endTime = null;
          boolean success = false;
          java.util.Date startTime = new java.util.Date();
          String WFCId = null;
          boolean connCreated=false;
          PreparedStatement pstmt=null;
          try {
             if (conn==null) {
                 conn=Conn.getConnection();
                 connCreated=true;
                 }
              String sql=JDBCService.getNamedSQL(conn,"wfm_forceterminate");
              pstmt=conn.prepareStatement(sql);
              WFCIds=getLastWFCs(wf_id,FORCE_TERMINATED);
              if (WFCIds != null && WFCIds.size()>0) {
                  for (int i=0; i<WFCIds.size(); i++ ) {
                      WFCId=(String) WFCIds.elementAt(i);
                      pstmt.clearParameters();
                      pstmt.setInt(1,WFCBase.WF_TERMINATED);
                      pstmt.setString(2,"Force Terminated at "+startTime.toString());
                      pstmt.setString(3,WFCId);
                      int rows=pstmt.executeUpdate();
                      if (rows !=1) {
                           success = false;
                           WFGlobals.out.logError( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_forceTerminate", new Object[]{ "" + rows,"" +wf_id , "" +WFCId});
                      } else {
                           success = true;
                           Event event = Event.findByWorkFlowContextId(WFCId);
                           if (event != null) {
                               if (event.remove(event)) {
                                    if (WFGlobals.out.debug) {
                                          WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "DEB_WorkFlowMonitor_forceTerminate" ,  new Object[]{ "" + event });
                                        }
                                    } else {
                                        WFGlobals.out.logError( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFlowMonitor_forceTerminate" ,  new Object[]{ "" + event });
                                        }
                                    event = null;
                                    }
                               } 
                           }
              } else {
                  WFGlobals.out.logError( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_forceterminate1" ,  new Object[]{ "" + wf_id });
              }
          } catch (SQLException se) {
              WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFLowMonitor_forceTerminateion" ,  new Object[]{ "" + wf_id }, se);
          } finally {
             try {
                if (pstmt !=null) {pstmt.close();}
             } catch (SQLException se) {
                WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFLowMonitor_forceTerminateWF",se); 
                }
             if (conn !=null && connCreated) {
                 Conn.freeConnection(conn);
                 }
          }
          return success;
      }

    public WorkFlowContext  read(Connection conn, String wfc_id) {

     boolean isConnNull=false;
     PreparedStatement pstmt=null;
     ResultSet rs=null;
     String sqlText=null;
     WorkFlowContext wfc=null;

    try {
       if (conn == null) {
          isConnNull=true;
          conn=Conn.getConnection();
          }
       sqlText=JDBCService.getNamedSQL(conn,"wfm_simpleread_wfc");
       pstmt = conn.prepareStatement(sqlText);
       pstmt.setString(1, wfc_id);
       rs = pstmt.executeQuery();

       if (rs.next()) {
                int wfd_id = rs.getInt(1);
                wfc=new WorkFlowContext(wfd_id);
                int wfd_version = rs.getInt(2);
                wfc.setWFDVersion(wfd_version);
                long workflow_id = rs.getLong(3);
                wfc.setWorkFlowId(workflow_id);
                int activityInfo_id = rs.getInt(4);
                wfc.setActivityInfoId(activityInfo_id);
                int nextActivityInfo_id = rs.getInt(5);
                wfc.setNextActivityInfoId(nextActivityInfo_id);
                String orig_wfc_id = rs.getString(6);
                wfc.setOrigWorkFlowContextId(orig_wfc_id);
                String prev_wfc_id = rs.getString(7);
                wfc.setPrevWorkFlowContextId(prev_wfc_id);
                int parent_WFD_id = rs.getInt(8);
                wfc.setParentWFDId(parent_WFD_id);
                int parent_WFD_version = rs.getInt(9);
                wfc.setParentWFDVersion(parent_WFD_version);
                String branch_id = rs.getString(10);
                wfc.setBranchId(branch_id);
                int step_id = rs.getInt(11);
                wfc.setStepId(step_id);
                String service_name = rs.getString(12);
                wfc.setServiceName(service_name);
                int basicStatus = rs.getInt(13);
                wfc.setBasicStatus( basicStatus);
                String advStatus = rs.getString(14);
                wfc.setAdvancedStatus(advStatus);
                int wfe_basic_status = rs.getInt(15);
                wfc.setWFEBasicStatus(wfe_basic_status);
                wfc.setWorkFlowContextId(wfc_id);
                Timestamp ts = rs.getTimestamp(16);
                wfc.setStartTime(ts.getTime() + ((long)ts.getNanos()/1000000));
                ts = rs.getTimestamp(17);
                wfc.setEndTime(ts.getTime() + ((long)ts.getNanos()/1000000));
                }
     }  catch (SQLException sqle) {
           WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFLowMonitor_read", sqle);
     } finally {
          try {
            if (rs != null) { rs.close(); }
            if (pstmt != null) { pstmt.close();}
          } catch (SQLException sse) {
               WFGlobals.out.logError( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFLowMonitor_read1");
               }
            if (isConnNull && conn != null) {
                Conn.freeConnection(conn);
                }
           }
      return wfc;
     }


/* this only give rough info. about the number of wfs in workflow_context table
errors, and it is called by UIGlobals.java
*/

      public ArrayList getWFSumInfo(Connection con) throws SQLException  {

           Connection conn =null;
           boolean passedConn=true;
           PreparedStatement pstmt = null;
           ResultSet rs=null;
           ArrayList stateWithIDs=new ArrayList();
           Hashtable entry=null;
           int totalError=0;
           int waiting=0;
           int waitingOnIO=0;
           int halted= 0;
           int halting=0;
           int interrupted=0;
           int complete=0;
           int active=0;

           String sql = null;

           try {

              if (con ==null) {
                  passedConn=false;
                  conn=Conn.getConnection("dbUIPool");
              } else {
                  conn=con;
                  }

            sql=JDBCService.getNamedSQL(conn,"wfm_getWFSumInfo_s");
            pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1,WorkFlowContext.ERROR);
            pstmt.setInt(2,WorkFlowContext.WFE_SYSTEM_ERROR);
            pstmt.setInt(3,WorkFlowContext.SERVICE_CONFIG_ERROR);
            pstmt.setInt(4,PlatformConstants.UNINDEXED_COMPUTE_FLAG);
            rs = pstmt.executeQuery();
            if (rs != null && rs.next()) {
                halted=rs.getInt(1);
                }

             // clear pstmt and rs before reusing them SR#1210457
             if ( rs != null )
             {
                rs.close();
                rs = null;
             }
             if ( pstmt != null )
             {
                pstmt.close();
                pstmt = null;
             }


            sql = JDBCService.getNamedSQL(conn,"wfm_getWFSumInfo_s1");
            pstmt = conn.prepareStatement(sql);
            pstmt.clearParameters();
            pstmt.setInt(1,WorkFlowContext.WF_INTERRUPT_MAN);
            pstmt.setInt(2,WorkFlowContext.WF_INTERRUPT_AUTO);
            pstmt.setInt(3,PlatformConstants.UNINDEXED_COMPUTE_FLAG);
            rs = pstmt.executeQuery();
            if (rs != null && rs.next()) {
                interrupted=rs.getInt(1);
                }

             // clear pstmt and rs before reusing them SR#1210457
             if ( rs != null )
             {
                rs.close();
                rs = null;
             }
             if ( pstmt != null )
             {
                pstmt.close();
                pstmt = null;
             }

            sql = JDBCService.getNamedSQL(conn,"wfm_getWFSumInfo_s2");
            pstmt = conn.prepareStatement(sql);
            pstmt.clearParameters();
            pstmt.setInt(1,WorkFlowContext.WAITING);
            pstmt.setInt(2,PlatformConstants.UNINDEXED_COMPUTE_FLAG);
            rs = pstmt.executeQuery();
            if (rs != null && rs.next()) {
                waiting=rs.getInt(1);
                }

             // clear pstmt and rs before reusing them SR#1210457
             if ( rs != null )
             {
                rs.close();
                rs = null;
             }
             if ( pstmt != null )
             {
                pstmt.close();
                pstmt = null;
             }


	     //sql = JDBCService.getNamedSQL(conn,"wfm_getWFSumInfo_s2");
            pstmt = conn.prepareStatement(sql);
            pstmt.clearParameters();
            pstmt.setInt(1,WorkFlowContext.WAITING_ON_IO);
            pstmt.setInt(2,PlatformConstants.UNINDEXED_COMPUTE_FLAG);
            rs = pstmt.executeQuery();
            if (rs != null && rs.next()) {
                waitingOnIO=rs.getInt(1);
	    }

             // clear pstmt and rs before reusing them SR#1210457
             if ( rs != null )
             {
                rs.close();
                rs = null;
             }
             if ( pstmt != null )
             {
                pstmt.close();
                pstmt = null;
             }


            sql  = JDBCService.getNamedSQL(conn,"wfm_getWFSumInfo_s3");
            pstmt = conn.prepareStatement(sql);
            pstmt.clearParameters();
            pstmt.setInt(1,PlatformConstants.UNINDEXED_COMPUTE_FLAG);
            rs = pstmt.executeQuery();
            if (rs != null && rs.next()) {
                halting=rs.getInt(1);
                }

             // clear pstmt and rs before reusing them SR#1210457
             if ( rs != null )
             {
                rs.close();
                rs = null;
             }
             if ( pstmt != null )
             {
                pstmt.close();
                pstmt = null;
             }

            sql  = JDBCService.getNamedSQL(conn,"wfm_getWFSumInfo_s4");
            pstmt = conn.prepareStatement(sql);
            pstmt.clearParameters();
            pstmt.setInt(1,WorkFlowContext.SUCCESS);
            pstmt.setInt(2,ActivityInfo.DONE);
            rs = pstmt.executeQuery();
            if (rs != null && rs.next()) {
                complete=rs.getInt(1);
                }

             // clear pstmt and rs before reusing them SR#1210457
             if ( rs != null )
             {
                rs.close();
                rs = null;
             }
             if ( pstmt != null )
             {
                pstmt.close();
                pstmt = null;
             }

            totalError= halting + halted +waiting +waitingOnIO +interrupted;
            active =complete+totalError;

            if (active>0) {
                entry=new Hashtable();
                entry.put ("STATE", new Integer (WorkFlowMonitor.ACTIVE) );
                entry.put ("SUM", new Integer (active) );
                stateWithIDs.add (entry );
                }

            if (halting>0) {
                entry=new Hashtable();
                entry.put ("STATE", new Integer (WorkFlowMonitor.HALTING) );
                entry.put ("SUM", new Integer (halting) );
                stateWithIDs.add (entry );
                }
            if ( halted>0) {
                entry=new Hashtable();
                entry.put ("STATE", new Integer (WorkFlowMonitor.HALTED) );
                entry.put ("SUM", new Integer (halted) );
                stateWithIDs.add (entry );
                }
            if (waiting>0) {
                 entry=new Hashtable();
                 entry.put ("STATE", new Integer (WorkFlowMonitor.WAITING) );
                 entry.put ("SUM", new Integer (waiting) );
                 stateWithIDs.add (entry );
                }

            if (waitingOnIO>0) {
                 entry=new Hashtable();
                 entry.put ("STATE", new Integer (WorkFlowMonitor.WAITING_ON_IO) );
                 entry.put ("SUM", new Integer (waitingOnIO) );
                 stateWithIDs.add (entry );
	    }


            if (interrupted>0) {
                entry=new Hashtable();
                entry.put ("STATE", new Integer (WorkFlowMonitor.INTERRUPTED_MAN) );
                entry.put ("SUM", new Integer (interrupted) );
                stateWithIDs.add (entry );
                }



            if (stateWithIDs != null && stateWithIDs.size()>0) {
               return stateWithIDs;
            } else {
               return null;
               }


            } catch (SQLException sqle) {
/*               WFGlobals.out.logException("WorkFLowMonitor.getAllIDs() caught sqlexception while retrieving all ids",sqle);*/
               WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFLowMonitor_getAllIDs",sqle);
               throw sqle;
            } finally {
                  if (rs != null) {
                       rs.close();
                       }
                  if (pstmt != null) {
                       pstmt.close();
                       }
                  if (!passedConn) {
                       Conn.freeConnection(conn,"dbUIPool");
                       }
                  }
         }

    public boolean stopWF(String wfId, long timestamp) {
        String msg = null;
        String urlStr = null;
        OpsServerRMIImpl osi = OpsServerRMIImpl.getInstance();
        OpsServerRMI osr = null;


        //NOTE: command should NOT be signed with a leading '/'
        String cmd = null;

        //NOTE:  Need to add that '/' back in
        URL url = null;
        boolean successFlag = false;

        if (osi != null) {
            osi.setInactiveWF(Long.parseLong(wfId),timestamp,
                              WorkFlowContext.WFE_WF_INSTANCE_STOPPED);
            successFlag = true;
        } else {
            // Outside the container
            // Must use HTTP to central ops.
            try {
                osr=(OpsServerRMI)JNDIService.lookupRMI("OpsServer_"+serverName);

                if (osr != null) {
                    urlStr = osr.getCentralOpsURL();

                    StringBuffer buf =new StringBuffer("setinactivewf?wf_id=");
                    buf.append(wfId);
                    buf.append("&name=");
                    buf.append(serverName);
                    buf.append("&timestamp=");
                    buf.append(timestamp);
                    buf.append("&description=");
                    buf.append(WorkFlowContext.WFE_WF_INSTANCE_STOPPED);

                    cmd = signOpsCmd(buf.toString());

//                  System.out.println("WorkFlowMonitor.stopWF: url = "
//                                     + urlStr + "/" + cmd);

                    if (WFGlobals.out.debug) {
/*                        WFGlobals.out.logDebug("WorkFlowMonitor.stopWF: url = "
                                               + urlStr + "/" + cmd);*/
                        WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "DEB_WorkFlowMonitor_stopWF" ,  new Object[]{ "" + urlStr , "" + cmd });
                    }

                    //System.out.println("cmd = "+cmd);
                    url = URL.fetch( urlStr + "/" + cmd );
                }

                if (url != null && url.resp_body != null) {
                    if (url.resp_body.equals("OK")) {
                        successFlag = true;
                    }

                    if (WFGlobals.out.debug) {
/*                        WFGlobals.out.logDebug("WorkFlowMonitor.stopWF: "+
                                               "url.resp_body= "+
                                               url.resp_body);*/
                        WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "DEB_WorkFlowMonitor_stopWF1" ,  new Object[]{ "" + url.resp_body });
                    }
                } else {
/*                    WFGlobals.out.logError("WorkFlowMonitor.stopWF "+
                                           "No URL body found giving up");*/
                    WFGlobals.out.logError( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFlowMonitor_stopWF");
                }
            } catch (Exception e) {
/*                WFGlobals.out.logException("WorkflowMonitor.stopWF", e);*/
                WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkflowMonitor_stopWF", e);
            }
        }
        return successFlag;
    }



    private String signOpsCmd(String command) {

        String cmd = null;
        int idx = -1;
        int lz = -1;

        // String apsig = null;
        String base64sig = null;
        String secKey = null;
        byte sig[] = null;
        byte out[] = null;
        StringBuffer buf = null;

        //System.out.println("command = " + command);

        if (command == null)
            return null;

        idx = command.indexOf('?');
        //System.out.println("idx = " + idx);

        if (idx>0) { // up to the qmark only
            cmd = command.substring(0, idx);
        } else if (idx < 0) { // no qmark found
            cmd = command;
        }

        //System.out.println("cmd = " + cmd);

        if (cmd == null || cmd.equals(""))
            return null;

        secKey = cmd+":"+String.valueOf(System.currentTimeMillis());

        try {
          sig = Util.createSignature(secKey);
        } catch (UnrecoverableKeyException e) {
/*            WFGlobals.out.logException("WorkflowMonitor.stopWF",e);*/
            WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkflowMonitor_stopWF1",e);
            e.printStackTrace();
        }

        lz = sig.length;
        lz = lz + (lz/3);

        while (lz%4 != 0)
            lz++;

        out = new byte[lz];
        Util.base64EncodeLine( sig, out, sig.length );

        try {
           base64sig = new String(out, "US-ASCII");
        }
        catch( IOException ioe )
        {
           // Should never happen as ASCII supposedly always supported
/*           WFGlobals.out.logException("ASCII not supported !!", ioe);*/
           WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_supported", ioe);

           // Try the default encoding...
           base64sig = new String(out);
        }

        buf = new StringBuffer(command);

        if (idx > 0) { // already have parms (i.e., '?' already there)
            buf.append("&sig=");
        } else { // no parms yet (i.e., add a '?')
            buf.append("?sig=");
        }

        buf.append(encodeURL( base64sig ) );
        buf.append("&seckey=");
        buf.append(encodeURL( secKey ));

        // if (LogService.out.debug) {
        //     LogService.logDebug("WFReportService: sign Ops cmd= "+buf);
        // }

        return buf.toString();
    }



      private PrivateKeyInfo opsKey = null;
      private PrivateKeyInfo getOpsKey() {
          if (opsKey != null) { return(opsKey); }
          synchronized (this) {
              if (opsKey == null) {
                  opsKey = PrivateKeyInfo.getInstanceByName(null, "OpsKey");
              }
              notifyAll();
          }
          return(opsKey);
      }


    private byte[] createSignature(String secKey) {
        PrivateKeyInfo pk = getOpsKey();
        byte[] signature = null;

        if ((pk != null) && (secKey != null)) {
            try {
                signature = SCIRSA.sign(secKey.getBytes("US-ASCII"), new SCIKeyReference(null, pk));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return signature;
    }


   private String encodeURL( String url ) {
       try {
           url = com.sterlingcommerce.refactor.util.URLEncoder.encode(url);
       } catch( Exception e ) {
          // Fall back on jvm encoder
           url = java.net.URLEncoder.encode(url);
       }

       return url;
   }

   /**
     *  For the list of given BPs this method populates the status flag
     *
     *  @param list List of workflow ids
     *  @param conn Database connecttion
     *
     *  @return HashMap Pair of workflow_id and status values
     */
    public HashMap getStatus(ArrayList list, Connection conn) {
        //long st = System.currentTimeMillis();
        int size = list.size();
        int ctr = 1;
        StringBuffer wfIdsList = new StringBuffer();
        Iterator itr = list.iterator();

        HashMap statusMap = new HashMap();

        while(itr.hasNext()) {
            if (ctr % 250 == 0) {
                //System.out.println("Counter: "+ctr);
                populateStatus(wfIdsList, conn, statusMap);
                wfIdsList = new StringBuffer();
            } else {
                wfIdsList.append((String)itr.next());
                wfIdsList.append(",");
            }

            ctr++;
        }

        if (wfIdsList.length() > 0) {
            populateStatus(wfIdsList, conn, statusMap);
        }

        //long et = System.currentTimeMillis();
        //System.out.println("Time taken by getStatus: "+(et-st));

        return statusMap;
    }

    /**
     *  Process the comma separted list of BPs and get their statuses
     *
     *  @param wfIdsList Comma separated BP id list
     *  @param conn Database connecttion
     *  @param statusMap WorkFlowId->Status Map
     */
    private void populateStatus(StringBuffer wfIdsList, Connection conn,
                                HashMap statusMap) {
        Statement stmt   = null;
        ResultSet rs = null;

        try {
            stmt = conn.createStatement();
            String middleSql = wfIdsList.toString();
            int len = middleSql.length();
            middleSql = middleSql.substring(0, len-1);
            if (middleSql.length() == 0) {
                return;
            }

            String sql = GET_STATUS_SQL_START+middleSql+GET_STATUS_SQL_END;
            //System.out.println("Processing Status SQL: "+sql);

            rs = stmt.executeQuery(sql);
            long prevWfId = -1;
            long currentWfId;
            int status;

            while (rs.next()) {
                currentWfId = rs.getLong(1);
                status = rs.getInt(2);

                //System.out.println("Current WFId: "+currentWfId+" Prev WFId: "+prevWfId);
                if (currentWfId != prevWfId) {
                    //System.out.println("WFId: "+currentWfId+" Status: "+status);
                    statusMap.put(new Long(currentWfId), new Integer(status));
                }

                prevWfId = currentWfId;
            }
        } catch (SQLException sqle) {
/*            LogService.out.logException("WorkFlowMonitor.populateStatus()", sqle);*/
            LogService.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFlowMonitor_populateStatus", sqle);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException sqle) {
/*                    LogService.out.logException("WorkFlowMonitor.populateStatus()" +
                        "-> Error Closing Statement", sqle);*/
                    LogService.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFlowMonitor_populateStatus1", sqle);
                }
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqle) {
/*                    LogService.out.logException("WorkFlowMonitor.populateStatus()" +
                        "-> Error Closing Statement", sqle);*/
                    LogService.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFlowMonitor_populateStatus11", sqle);
                }
            }
        }
    }

    public static Object[] getStateAndStatusSql(int[] states, int[] statuses,

        ArrayList workflowDefIds, ArrayList workflowIds,
        Timestamp startTime, Timestamp endTime,
        Timestamp deadlineStartTime, Timestamp deadlineEndTime,
        int maxKeys, WorkFlowManager wfmgr )

      throws Exception {

        //System.out.println("States: "+states+" Statuses: "+statuses+" workflowDefIds: "+workflowDefIds+" workflowIds: "+workflowIds+" startTime: "+startTime+" endTime: "+endTime+" maxKeys: "+maxKeys);

        boolean active = false;
        boolean active_waiting = false;
        boolean complete = false;
        boolean terminated = false;
        boolean waiting = false;
        boolean waitingOnIO = false;
        boolean halted = false;
        boolean halting = false;
        boolean interrupted_auto = false;
        boolean interrupted_man = false;
        boolean noTime = false;

        Object[] ret  = new Object[2];

        if ((startTime == null)  && (endTime == null))
        {
          noTime = true;
        }


        if(states != null) {
          int state = -1;
          for(int i = 0; i < states.length; i++) {

            state = states[i];
            //System.out.println("Processing state: "+state);
            switch(state) {

              case ACTIVE:
              case ACTIVE_WAITING:
                active = true;
                break;
              case COMPLETE:
                complete = true;
                break;
              case TERMINATED:
                terminated = true;
                break;
              case WAITING:
                waiting = true;
                break;
              case WAITING_ON_IO:
                waitingOnIO = true;
                break;
              case HALTED:
                halted = true;
                break;
              case HALTING:
                halting = true;
                break;
              case INTERRUPTED_AUTO:
                interrupted_auto = true;
                break;
              case INTERRUPTED_MAN:
                interrupted_man = true;
                break;
              default:
                throw new Exception("UNKNOWN STATE");
                //throw new Exception(WFGlobals.out.getlogMessageStr( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFlowMonitor_Exception2_str2"));
            }
          }
        }

        boolean error = false;
        boolean success = false;

        if(statuses != null) {
          for(int i = 0; i < statuses.length; i++) {

            int status = statuses[i];
            //System.out.println("Processing status: "+status);
            switch(status) {

              case WorkFlowContext.ERROR:
                error = true;
                break;
              case WorkFlowContext.SUCCESS:
                success = true;
                break;
              default:
                throw new Exception("UNKNOWN STATUS");
                //throw new Exception(WFGlobals.out.getlogMessageStr( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_WorkFlowMonitor_Exception3_str3"));

            }
          }
        }

        boolean hasWorkflowDefIds = false;
        boolean badState = false;
        boolean badStatus = false;
        boolean onlyComplete = false;
        String hint = null;

        if ( workflowDefIds != null && workflowDefIds.size() > 0) {
          hasWorkflowDefIds = true;
        }

        boolean activeOrHalting = false;

        if ( terminated || halted || interrupted_auto ||
            interrupted_man || waiting || waitingOnIO) {
          badState = true;
        }

        if( error && !success) {
          badStatus = true;
        }

        if(error && success) {
           statuses = null;
        }

        if ( active || halting ) {
          activeOrHalting = true;
        }

        //System.out.println("badState: "+badState+" badStatus: "+badStatus+" activeOrHalting: "+activeOrHalting);




        boolean defaultQuery = false;

        StringBuffer sb = new StringBuffer();
        boolean otherFilters = false;

        sb.append(" SELECT ");

        if( activeOrHalting ) {

          if(badStatus) {

            if(hasWorkflowDefIds) {
              hint = "getStateAndStatus_HINT_ACTIVE_BADSTATUS_WFD";
            } else {
              hint = "getStateAndStatus_HINT_ACTIVE_BADSTATUS";
            }
            sqlHint(sb, maxKeys, hint);
            if(orderByWorkFlowId) {
              sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_DEFAULT_BEGIN"));
            } else {
              sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_DERIVED_BEGIN"));
            }
            sb.append(" ");
          sqlHint(sb, maxKeys, hint);
            sb.append(" ");
            sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_MAIN"));
            sb.append(" ");
            sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_BADSTATUS_MIDDLE"));
            sb.append(" ");
          }
          else if (badState) {
          if(hasWorkflowDefIds) {
            hint = "getStateAndStatus_HINT_ACTIVE_WFD";
          } else {
            hint = "getStateAndStatus_HINT_ACTIVE";
          }
          sqlHint(sb, maxKeys, hint);
            sb.append(" ");
            if(orderByWorkFlowId) {
              sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_DEFAULT_BEGIN"));
            } else {
              sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_DERIVED_BEGIN"));
            }
            sb.append(" ");
          sqlHint(sb, maxKeys, hint);
            sb.append(" ");
            sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_MAIN"));
            sb.append(" ");
            sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_DEFAULT_MIDDLE"));
            sb.append(" ");
          }
          else {
          if(hasWorkflowDefIds) {
            hint = "getStateAndStatus_HINT_ACTIVE_WFD";
          } else {
            hint = "getStateAndStatus_HINT_ACTIVE";
          }
          sqlHint(sb, maxKeys, hint);
            sb.append(" ");
            sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_DEFAULT_BEGIN"));
            sb.append(" ");
            sqlHint(sb, maxKeys, hint);
            sb.append(" ");
            sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_MAIN"));
            sb.append(" ");
            sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_DEFAULT_MIDDLE"));
            sb.append(" ");
          }
          otherFilters = true;

        }
        else if( complete && badState && badStatus ) {

          if(hasWorkflowDefIds) {
            hint = "getStateAndStatus_HINT_COMPLETE_BADSTATE_BADSTATUS_WFD";
          } else {
            hint = "getStateAndStatus_HINT_COMPLETE_BADSTATE_BADSTATUS";
          }
          sqlHint(sb, maxKeys, hint);
            sb.append(" ");
            sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_DEFAULT_BEGIN"));
            sb.append(" ");
          sqlHint(sb, maxKeys, hint);
            sb.append(" ");
          sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_MAIN"));
            sb.append(" ");
          sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_COMPLETE_BADSTATE_BADSTATUS_MIDDLE"));
            sb.append(" ");
          setBadStateInClause(sb, waiting, waitingOnIO, interrupted_auto,
			      interrupted_man, halted, terminated);
            sb.append(" ");

          // JAK for SR 1344183 if the start time and end time are both null
          //  then use modified query without exists otherwise use standard
          // query -- for sr 1344183
          if (noTime)
          {
            sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_COMPLETE_BADSTATE_BADSTATUS_MIDDLE_2_NOTIME"));
          }
          else
          {
            sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_COMPLETE_BADSTATE_BADSTATUS_MIDDLE_2"));
            otherFilters = true;
          }

        sb.append(" ");
        }
        else if( complete && !badState && badStatus ) {

          if(hasWorkflowDefIds) {
            hint = "getStateAndStatus_HINT_COMPLETE_BADSTATUS_WFD";
          } else {
            hint = "getStateAndStatus_HINT_COMPLETE_BADSTATUS";
          }
          sqlHint(sb, maxKeys, hint);
            sb.append(" ");
            sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_DEFAULT_BEGIN"));
            sb.append(" ");
          sqlHint(sb, maxKeys, hint);
            sb.append(" ");
          sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_MAIN"));
            sb.append(" ");

          //
          // JAK if start time and end time are null use the version of the sql
          // without the exists otherwise use the regular sql
          //
          if (noTime)
          {
            sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_COMPLETE_BADSTATUS_MIDDLE_NOTIME"));
          }
          else
          {
            sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_COMPLETE_BADSTATUS_MIDDLE"));
            otherFilters = true;
          }
          sb.append(" ");
        }
        else if( complete && badState && !badStatus ) {

          if(hasWorkflowDefIds) {
            hint = "getStateAndStatus_HINT_COMPLETE_BADSTATUS_WFD";
          } else {
            hint = "getStateAndStatus_HINT_COMPLETE_BADSTATE";
          }
          sqlHint(sb, maxKeys, hint);
            sb.append(" ");
          sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_DEFAULT_BEGIN"));
            sb.append(" ");
          sqlHint(sb, maxKeys, hint);
            sb.append(" ");
          sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_MAIN"));
            sb.append(" ");
          sb.append(" ");
          sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_COMPLETE_BADSTATE_MIDDLE"));
            sb.append(" ");
          setBadStateInClause(sb, waiting, waitingOnIO, interrupted_auto,
			      interrupted_man, halted, terminated);
            sb.append(" ");
          sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_COMPLETE_BADSTATE_MIDDLE_2"));
            sb.append(" ");

          otherFilters = true;

        }
        else if( complete && !badState && !badStatus ) {

          if(hasWorkflowDefIds) {
            hint = "getStateAndStatus_HINT_COMPLETE_WFD";
          } else {
            hint = "getStateAndStatus_HINT_COMPLETE";
          }
          sqlHint(sb, maxKeys, hint);
            sb.append(" ");
          sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_DEFAULT_BEGIN"));
            sb.append(" ");
          sqlHint(sb, maxKeys, hint);
            sb.append(" ");
          sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_MAIN"));
            sb.append(" ");
          sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_COMPLETE_MIDDLE"));
            sb.append(" ");
          otherFilters = true;

        }
        else if( badState && badStatus ) {

          if(hasWorkflowDefIds) {
            hint = "getStateAndStatus_HINT_BADSTATE_BADSTATUS_WFD";
          } else {
            hint = "getStateAndStatus_HINT_BADSTATE_BADSTATUS";
          }
          sqlHint(sb, maxKeys, hint);
            sb.append(" ");
            if(orderByWorkFlowId) {
              sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_DEFAULT_BEGIN"));
            } else {
              sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_DERIVED_BEGIN"));
            }
            sb.append(" ");
          sqlHint(sb, maxKeys, hint);
            sb.append(" ");
          sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_MAIN"));
          sb.append(" ");
          sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_BADSTATE_BADSTATUS_MIDDLE"));
            sb.append(" ");
          setBadStateInClause(sb, waiting, waitingOnIO, interrupted_auto,
			      interrupted_man, halted, terminated);
            sb.append(" ");
          sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_BADSTATE_BADSTATUS_MIDDLE_2"));
          sb.append(" ");
          otherFilters = true;
        }
        else if( !badState && badStatus ) {

          if(hasWorkflowDefIds) {
            hint = "getStateAndStatus_HINT_BADSTATUS_WFD";
          } else {
            hint = "getStateAndStatus_HINT_BADSTATUS";
          }
          sqlHint(sb, maxKeys, hint);
            sb.append(" ");
            if(orderByWorkFlowId) {
              sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_DEFAULT_BEGIN"));
            } else {
              sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_DERIVED_BEGIN"));
            }
            sb.append(" ");
          sqlHint(sb, maxKeys, hint);
            sb.append(" ");
          sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_MAIN"));
            sb.append(" ");

          // JAK for sr 1344183 if start time and end time are both null
          // then append extra blank as opposed to the exists. Otherwise use the
          // standard query

          if (noTime)
          {
            sb.append(" ");
          }
          else
          {
            sb.append(" ");
            sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_BADSTATUS_MIDDLE"));
            otherFilters = true;
          }
        }
        else if( badState && !badStatus ) {
          if(hasWorkflowDefIds) {
            hint = "getStateAndStatus_HINT_BADSTATE_WFD";
          } else {
            hint = "getStateAndStatus_HINT_BADSTATE";
          }
          sqlHint(sb, maxKeys, hint);
            sb.append(" ");
            if(orderByWorkFlowId) {
              sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_DEFAULT_BEGIN"));
            } else {
              sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_DERIVED_BEGIN"));
            }
            sb.append(" ");
          sqlHint(sb, maxKeys, hint);
            sb.append(" ");
          sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_MAIN"));
            sb.append(" ");

          //
          // JAK for sr 1344183 if the start time and end time are both null
          // then append an extra blank as sopposed to the exists.
          // ... otherwise use the standard query
          //
          if (noTime)
          {
            sb.append(" ");
          }
          else
          {
            sb.append(" ");
            sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_BADSTATE_MIDDLE"));
            otherFilters = true;
          }
            sb.append(" ");
        }
        else {
          //Default
          sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_DEFAULT_BEGIN"));
          if(hasWorkflowDefIds) {
            hint = "getStateAndStatus_HINT_DEFAULT_WFD";
          } else {
            hint = "getStateAndStatus_HINT_DEFAULT";
          }
          sqlHint(sb, maxKeys, hint);
            sb.append(" ");
          sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_MAIN"));
          sb.append(" ");
          sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_DEFAULT_MIDDLE"));
            sb.append(" ");
          otherFilters = true;
        }

        if(hasWorkflowDefIds) {

          int wfDefSize = workflowDefIds.size();
          for(int i = 0; i < wfDefSize; i++) {
            if(i == 0) {
              if(otherFilters) {
                sb.append(" AND ");
              }
              else {
                sb.append(" WHERE ");
                otherFilters = true;
              }
              sb.append(" WFD_ID IN ( ");
              sb.append(workflowDefIds.get(i));
            }
            else {
              sb.append(",");
              sb.append(workflowDefIds.get(i));
            }
          }
          if( wfDefSize > 0 ) {
            sb.append(" ) ");
          }
        }

        if(startTime != null && endTime != null) {
          if(otherFilters) {
            sb.append(" AND ");
          }
          else {
            sb.append(" WHERE ");
            otherFilters = true;
          }
          sb.append(" START_TIME BETWEEN ");
          WorkFlowMonitor.setDate(sb, startTime);
          sb.append(" AND ");
          WorkFlowMonitor.setDate(sb, endTime);
        }
        else if(startTime != null) {
          if(otherFilters) {
            sb.append(" AND ");
          }
          else {
            sb.append(" WHERE ");
            otherFilters = true;
          }
          sb.append(" START_TIME >= ");
          WorkFlowMonitor.setDate(sb, startTime);
        }
        else if(endTime != null) {
          if(otherFilters) {
            sb.append(" AND ");
          }
          else {
            sb.append(" WHERE ");
            otherFilters = true;
          }
          sb.append(" START_TIME <= ");
          WorkFlowMonitor.setDate(sb, endTime);
        }

        if(wfmgr != null && deadlineStartTime != null &&
            deadlineEndTime != null){
          //deadline related
          wfmgr.addDeadlineToQueryNew(deadlineStartTime,
              deadlineEndTime,
              sb, false);
        }
        //end deadline

        //else if( activeOrHalting && !badStatus) {
        //  sb.append(" AND START_TIME <= ");
        //  setDate(sb, new Timestamp(System.currentTimeMillis()));
        //}

        if( badStatus || (badState && !complete && !activeOrHalting)) {

          //
          // JAK for sr 1344183 if start time and end time are both null then
          // append a blank as opposed to an ending parenthesis for the exists
          //
          //

          if (noTime)
          {
            sb.append(" ");
          }
          else
          {
            sb.append(" ) ");
          }
        }

        if(activeOrHalting && !badState && !complete ) {
          //Only active or halting

          if(otherFilters) {
            sb.append(" AND ");
          }
          else {
            sb.append(" WHERE ");
            otherFilters = true;
          }

          sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_ARCHIVE_INFO_SUBSELECT"));
        }

        if(badStatus) {

          if(otherFilters) {
            sb.append(" AND ");
          }
          else {
            sb.append(" WHERE ");
            otherFilters = true;
          }

          sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_BADSTATUS_END"));

        }
        else if(badState && !complete && !activeOrHalting) {

          if(otherFilters) {
            sb.append(" AND ");
          }
          else {
            sb.append(" WHERE ");
            otherFilters = true;
          }
          setBadStateInClause(sb, waiting, waitingOnIO, interrupted_auto,
              interrupted_man, halted, terminated);
        }


        if ( workflowIds != null && workflowIds.size() > 0) {
          int wfSize = workflowIds.size();
          for(int i = 0; i < wfSize; i++) {
            if(i == 0) {

              if(otherFilters) {
                sb.append(" AND ");
              }
              else {
                sb.append(" WHERE ");
                otherFilters = true;
              }
              sb.append(" WORKFLOW_ID IN ( ");
              sb.append(workflowIds.get(i));
            }
            else {
              sb.append(",");
              sb.append(workflowIds.get(i));
            }
          }
          if( wfSize > 0 ) {
            sb.append(" ) ");
          }

        }


        if(!useInformixSyntax) {
          if((badState && !complete) || badStatus) {
            if(orderByWorkFlowId) {
              sb.append(" ) SUBQRY ");
            }
            else {
              sb.append(" ) WFC_VIEW ");
            }
          }
          else {
            sb.append(" ) SUBQRY ");
          }

        }

        if(!useInformixSyntax) {
            otherFilters = false;
        }

        otherFilters = appendStateAndStatus(states, statuses, otherFilters, sb);


        sb.append(" ");

        if((badState && !complete) || badStatus) {

          if(orderByWorkFlowId) {
            sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_ORDER_BY_WFID"));
          } else {
            sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_ORDER_BY_DERIVED"));
          }

        }
        else {
          sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_ORDER_BY"));
        }
        sb.append(" ");

        ret[0] = sb.toString();
        return ret;
      }

      public static void setBadStateInClause(StringBuffer sb, boolean waiting,
          boolean waitingOnIO, boolean interrupted_auto, boolean interrupted_man,
          boolean halted, boolean terminated) {

        boolean others = false;
        sb.append(" ( BASIC_STATUS IN ( ");
        if(terminated) {
          sb.append(WorkFlowContext.WF_TERMINATED);
          others = true;
        }

        if(waiting) {
          if(others) sb.append(",");
          sb.append(WorkFlowContext.WAITING);
          others = true;
        }

        if(waitingOnIO) {
          if(others) sb.append(",");
          sb.append(WorkFlowContext.WAITING_ON_IO);
          others = true;
        }

        if(interrupted_auto) {
          if(others) sb.append(",");
          sb.append(WorkFlowContext.WF_INTERRUPT_AUTO);
          others = true;
        }

        if(interrupted_man) {
          if(others) sb.append(",");
          sb.append(WorkFlowContext.WF_INTERRUPT_MAN);
          others = true;
        }

        if(halted) {
          if(others) sb.append(",");
          sb.append(WorkFlowContext.WFE_SYSTEM_ERROR);
          sb.append(",");
          sb.append(WorkFlowContext.SYSTEM_ERROR);
          sb.append(",");
          sb.append(WorkFlowContext.SERVICE_CONFIG_ERROR);
          sb.append(",");
          sb.append(WorkFlowContext.WARNING);
          sb.append(",");
          sb.append(WorkFlowContext.ERROR);
          others = true;
        }

        if(others) sb.append(" ) ");

        /*
           if(halted) {

           if(terminated || waiting || interrupted_auto || interrupted_man ) {
           sb.append(" OR ( BASIC_STATUS IN ( ");
           }

           sb.append(WorkFlowContext.WFE_SYSTEM_ERROR);
           sb.append(",");
           sb.append(WorkFlowContext.SYSTEM_ERROR);
           sb.append(",");
           sb.append(WorkFlowContext.SERVICE_CONFIG_ERROR);
           sb.append(",");
           sb.append(WorkFlowContext.WARNING);
           sb.append(",");
           sb.append(WorkFlowContext.ERROR);
           sb.append(" ) ");

           sb.append(" AND ");

           sb.append(" ( WFE_STATUS IN ( ");
           sb.append(WorkFlowContext.WFE_WFD_DEACTIVATED);
           sb.append(",");
           sb.append(WorkFlowContext.WFE_SERVICE_DEACTIVATED);
           sb.append(",");
           sb.append(WorkFlowContext.WFE_REMOTE_ERROR);
           sb.append(",");
           sb.append(WorkFlowContext.WFE_CREATE_ERROR);
           sb.append(",");
           sb.append(WorkFlowContext.WFE_BASIC_ERROR);
           sb.append(",");
           sb.append(WorkFlowContext.WFE_NAME_ERROR);
           sb.append(",");
           sb.append(WorkFlowContext.WFE_LICENSE_ERROR);
           sb.append(",");
           sb.append(WorkFlowContext.WFE_JMS_ERROR);
           sb.append(" ) ");
           sb.append(" OR ACTIVITYINFO_ID = NEXT_AI_ID ) ");

           if(terminated || waiting || interrupted_auto || interrupted_man ) {
           sb.append(" ) ");
           }
           }
         */

        sb.append(" ) ");

    }


      private static void appendStateList(
          int[] states, StringBuffer sb) {


        if(states != null && states.length > 0) {

          sb.append(" IN ( ");

          for(int i = 0; i < states.length; i++) {
            if(i == 0) {
              if(states[i] == ACTIVE) {
                sb.append(getPrecedenceOrder(ACTIVE));
                sb.append(",");
                sb.append(getPrecedenceOrder(ACTIVE_WAITING));
              }
              else {
                sb.append(getPrecedenceOrder(states[i]));
              }
            }
            else {
              if(states[i] == ACTIVE) {
                sb.append(",");
                sb.append(getPrecedenceOrder(ACTIVE));
                sb.append(",");
                sb.append(getPrecedenceOrder(ACTIVE_WAITING));
              }
              else {
                sb.append(",");
                sb.append(getPrecedenceOrder(states[i]));
              }
            }
          }
          sb.append(" ) ");
        }

    }

    private static void appendStatusList(
        int[] statuses, StringBuffer sb) {

      if(statuses != null && statuses.length > 0 ) {

        for(int i = 0; i < statuses.length; i++) {

          sb.append(" IN ( ");

          if(i == 0) {
            sb.append(statuses[i]);
          }
          else {
            sb.append(",");
            sb.append(statuses[i]);
          }
        }
        sb.append(" ) ");
      }
    }

    private static void appendWhereAndCondition(boolean otherFilters, StringBuffer sb)  {
      if(otherFilters) {
        sb.append(" AND ");
      }
      else {
        sb.append(" WHERE ");
      }
    }

    private static boolean appendStateAndStatus(
        int[] states, int[] statuses, boolean otherFilters, StringBuffer sb) {

      if(states != null && states.length > 0 ) {
        appendWhereAndCondition(otherFilters, sb);
        otherFilters = true;

        sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_STATE_FILTER"));

        appendStateList(states, sb);
      }

      if(statuses != null && statuses.length > 0 ) {
        appendWhereAndCondition(otherFilters, sb);
        otherFilters = true;

        sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_STATUS_FILTER"));

        appendStatusList(statuses, sb);
      }

      return otherFilters;
    }



/*
    private static void appendLimitedMainQuery(
        int[] states, int[] statuses, StringBuffer sb) {

      sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_MAIN"));
      if(useInformixSyntax) {
        sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_MAIN_2"));
        appendStateAndStatus(states, statuses, sb);
        sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_MAIN_3"));
      }
    }

    private static void appendMainQuery(StringBuffer sb) {
      sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_MAIN"));
      if(useInformixSyntax) {
        sb.append(JDBCService.getNamedSQL(dbPool, "getStateAndStatus_MAIN_3"));
      }
    }
*/

    public ArrayList getIdsByStateAndStatus(int maxKeys,
                            int[] states, int[] statuses ) {

      ArrayList idList = null;

      Connection conn = null;
      ResultSet rs = null;
      PreparedStatement ps = null;

      try {
        String sql = (String) getStateAndStatusSql(states, statuses,
                                          null, null, null, null,
                                          null, null,
                                          maxKeys, null)[0];
        //System.out.println(sql);
        if(WFGlobals.out.debug) {
          String msg = myClassName + ".getIdsByStateAndStatus() query " + sql;
/*          WFGlobals.out.logDebug(msg);*/
          WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_msg6" ,  new Object[]{ "" + msg });
/*          WFGlobals.out.logDebug("\nMax fetch Size="+maxBPsToDisplay);*/
          WFGlobals.out.logDebug( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "DEB_Size_maxBPsToDisplay1" ,  new Object[]{ "" + maxBPsToDisplay });
        }
        conn = Conn.getConnection();
        ps = conn.prepareStatement(sql);

        if(maxKeys != -1) {
          ps.setFetchSize(maxKeys);
        }
        else {
          ps.setFetchSize(GET_STATE_FETCH_SIZE);
        }

        rs = ps.executeQuery();

        idList = new ArrayList();
        TreeSet ts = new TreeSet();

        long lastId = -1;
        int lastState = -1;
        int numFound = 0;
	long wfId = -1;
        while((maxKeys == -1 || numFound < maxKeys)
                    && rs != null && rs.next() ) {

          wfId = rs.getLong("WORKFLOW_ID");

	  //System.out.println("wfId = "+wfId);
          if( lastId != wfId && lastId != -1
              && !ts.contains(new Long(lastId))) {

            idList.add(Long.toString(lastId));
            ts.add(new Long(lastId));
            numFound++;
          }
          lastId = wfId;
        }

        if((maxKeys == -1 || numFound < maxKeys) && lastId != -1
             && !ts.contains(new Long(lastId))) {

          idList.add(Long.toString(lastId));
          numFound++;
        }


      }  catch(SQLException sqe) {
        String msg = myClassName + ".getIdsByStateAndStatus() caught SQLException.";
/*        WFGlobals.out.logException(msg, sqe);*/
        WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getIdsByStateAndStatus" ,  new Object[]{ "" + myClassName }, sqe);
        sqe.printStackTrace();
      }  catch (Exception e) {
        String msg = myClassName + ".getIdsByStateAndStatus() caught Exception.";
/*        WFGlobals.out.logException(msg, e);*/
        WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getIdsByStateAndStatus1" ,  new Object[]{ "" + myClassName }, e);
        e.printStackTrace();
      } finally {
        try {
          if (rs != null) {
            rs.close();
          }

          if (ps != null) {
            ps.close();
          }
        } catch (SQLException sqe) {
          String msg = myClassName +
            ".getIdsByStateAndStatus(state) caught SQLException" +
            " while trying to close prepared statement.";
/*          WFGlobals.out.logException(msg, sqe);*/
          WFGlobals.out.logException( WFGlobals.WORKFLOW , WFGlobals.WORKFLOW , "ERR_getIdsByStateAndStatus_state" ,  new Object[]{ "" + myClassName }, sqe);
          sqe.printStackTrace();
        }

        if (conn != null) {
          Conn.freeConnection(conn);
        }
      }
      return idList;
    }

    public static int getPrecedenceOrder(int state) {

      int ret = UNKNOWN;
      switch(state) {
        case ACTIVE:
          ret = PRECEDENCE_ACTIVE;
          break;
        case COMPLETE:
          ret = PRECEDENCE_COMPLETE;
          break;
        case TERMINATED:
          ret = PRECEDENCE_TERMINATED;
          break;
        case WAITING:
          ret = PRECEDENCE_WAITING;
          break;
        case WAITING_ON_IO:
          ret = PRECEDENCE_WAITING_ON_IO;
          break;
        case HALTED:
          ret = PRECEDENCE_HALTED;
          break;
        case HALTING:
          ret = PRECEDENCE_HALTING;
          break;
        case INTERRUPTED_AUTO:
          ret = PRECEDENCE_INTERRUPTED_AUTO;
          break;
        case INTERRUPTED_MAN:
          ret = PRECEDENCE_INTERRUPTED_MAN;
          break;
        case ACTIVE_WAITING:
          ret = PRECEDENCE_ACTIVE_WAITING;
          break;
        default:
          break;
      }
      return ret;
    }

    public static int reversePrecedenceOrder(int state) {

      int ret = UNKNOWN;
      switch(state) {
        case PRECEDENCE_ACTIVE:
          ret = ACTIVE;
          break;
        case PRECEDENCE_COMPLETE:
          ret =  COMPLETE;
          break;
        case PRECEDENCE_TERMINATED:
          ret = TERMINATED;
          break;
        case PRECEDENCE_WAITING:
          ret = WAITING;
          break;
        case PRECEDENCE_WAITING_ON_IO:
          ret = WAITING_ON_IO;
          break;
        case PRECEDENCE_HALTED:
          ret = HALTED;
          break;
        case PRECEDENCE_HALTING:
          ret = HALTING;
          break;
        case PRECEDENCE_INTERRUPTED_AUTO:
          ret = INTERRUPTED_AUTO;
          break;
        case  PRECEDENCE_INTERRUPTED_MAN:
          ret = INTERRUPTED_MAN;
          break;
        case PRECEDENCE_ACTIVE_WAITING:

          ret = ACTIVE;
          break;
        default:

          break;
      }

      return ret;
    }

    // Oracle, MSSQL and DB2 only for now
    public static void setDate(StringBuffer sb, Timestamp ts) {

      //Bind variables have a "skewedness" problem.  Optimizer
      //runs before they are bound. Setting timestamp explicitly

      //Oracle9i has a problem with the TO_TIMESTAMP function.
      //Some people have reported it not using indexes
      //In my case, it would run fast full scans.

      //Remove the nanos value
      String x = ts.toString();

      String t =  x.substring(0, x.lastIndexOf("."));
      if (useMSSQLConvert) { //MSSQL
        sb.append(" CONVERT(DATETIME, '");
        sb.append(t);
        sb.append("', 120) ");
      } else if (useInformixSyntax) { //Informix
        sb.append(" TO_DATE('");
        sb.append(t);
        sb.append("', '%Y-%m-%d %H:%M:%S') ");
      } else { // Oracle and DB2
        sb.append(" TO_DATE('");
        sb.append(t);
        sb.append("', 'YYYY-MM-DD HH24:MI:SS') ");
      }
    }

    // Oracle only
    public static void sqlHint(StringBuffer sb, int maxKeys, String hint ) {
        if(maxKeys != -1 ) {
           if(useOracle9iFirstRows) {
            sb.append(" /*+ ");
            sb.append(" FIRST_ROWS(");
            sb.append(2*maxKeys);
            sb.append(") ");
            sb.append(" */ ");
           } else {
            String h = JDBCService.getNamedSQL(dbPool, hint);
            if(h != null) {
              sb.append(" ");
              sb.append(h);
            }
            sb.append(" ");
           }
        } else {
           sb.append(" ");
        }
    }
}
