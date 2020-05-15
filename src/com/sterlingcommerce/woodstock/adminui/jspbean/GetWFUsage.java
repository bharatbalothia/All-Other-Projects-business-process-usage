package com.sterlingcommerce.woodstock.adminui.jspbean;

import com.sterlingcommerce.woodstock.ui.BaseUIGlobals;

import java.util.ArrayList;

import com.sterlingcommerce.woodstock.util.frame.jdbc.Conn;

import java.util.Hashtable;

import java.sql.*;

import com.sterlingcommerce.woodstock.util.frame.jdbc.JDBCService;

public class GetWFUsage {
	
	
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
    

	public String getWFUsage() {
		StringBuffer s = new StringBuffer();

		ArrayList wfarray = BaseUIGlobals.getActualWorkFlowUsage();
		if (wfarray != null) {
			for (int i = 0; i < wfarray.size(); i++) {
				Hashtable entry = (Hashtable) wfarray.get(i);
				int state = ((Integer) entry.get("STATE")).intValue();
				ArrayList al = (ArrayList) entry.get("IDS");

				int numbps = al.size();

				switch (state) {
				case 0:
					s.append("Active");
					break;
				case 5:
					s.append("Halting");
					break;
				case 4:
					s.append("Halt");
					break;
				case 3:
					s.append("Waiting");
					break;
				case 7:
					s.append("Int_man");
					break;
				case 6:
					s.append("Int");
					break;
				case 10:
					s.append("WaitingOnIO");
					break;
				case 11:
					s.append("AsyncQueued");
					break;
				case 12:
					s.append("Softstop");
					break;
				}

			}
		}

		return s.toString();
	}

	public ArrayList getAllIdsWithStateNew(Connection con, int num) {

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
		String sql = null;
		boolean passedIn = false;
		try {
			if (con == null) {
				conn = Conn.getConnection();
			} else {
				conn = con;
				passedIn = true;
			}
			sb.append(" SELECT ");
			sb.append(JDBCService.getNamedSQL(conn, "getStateAndStatus_MAIN"));
			sb.append(" ");
			sb.append(JDBCService.getNamedSQL(conn, "wfm_getAllIdsWithStateNew_where"));
			sql = sb.toString();

			pstmt = conn.prepareStatement(sql);

			rs = pstmt.executeQuery();

			int numFound = 0;

			while (rs != null && rs.next()) {
				String wfId = rs.getString("WORKFLOW_ID");
				int state = rs.getInt("STATE");

				switch (reversePrecedenceOrder(state)) {
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
				default:
					break;
				}
				// idList.add(wfId);
				numFound++;

				if (num != -1 && numFound >= num) {
					// Keep going in the infinite
					// case until result set exhausted or break on n case
					break;
				}
			}

			Hashtable active = new Hashtable();
			active.put("STATE", new Integer(ACTIVE));
			active.put("IDS", activeList);
			idList.add(active);

			Hashtable halted = new Hashtable();
			halted.put("STATE", new Integer(HALTED));
			halted.put("IDS", haltedList);
			idList.add(halted);

			Hashtable halting = new Hashtable();
			halting.put("STATE", new Integer(HALTING));
			halting.put("IDS", haltingList);
			idList.add(halting);

			Hashtable waiting = new Hashtable();
			waiting.put("STATE", new Integer(WAITING));
			waiting.put("IDS", waitingList);
			idList.add(waiting);

			Hashtable waitingOnIO = new Hashtable();
			waitingOnIO.put("STATE", new Integer(WAITING_ON_IO));
			waitingOnIO.put("IDS", waitingOnIOList);
			idList.add(waitingOnIO);

			Hashtable interrupted = new Hashtable();
			interrupted.put("STATE", new Integer(INTERRUPTED_MAN));
			interrupted.put("IDS", interruptedManList);
			idList.add(interrupted);

			Hashtable interruptedAuto = new Hashtable();
			interruptedAuto.put("STATE", new Integer(INTERRUPTED_AUTO));
			interruptedAuto.put("IDS", interruptedAutoList);
			idList.add(interruptedAuto);

		} catch (SQLException sqe) {
			sqe.printStackTrace();
		} catch (Exception e) {
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
				sqe.printStackTrace();
			}

			if (conn != null && !passedIn) {
				Conn.freeConnection(conn);
			}
		}
		return idList;

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

}
