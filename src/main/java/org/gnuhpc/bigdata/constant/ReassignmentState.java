package org.gnuhpc.bigdata.constant;

public enum ReassignmentState {
  ReassignmentFailed(-1, "Reassignment Failed"),
  ReassignmentInProgress(0, "Reassignment In Progress"),
  ReassignmentCompleted(1, "Reassignment Completed");

  private int status;
  private String msg;

  private ReassignmentState(int status, String msg) {
    this.status = status;
    this.msg = msg;
  }

  public static ReassignmentState valueOf(int status) {
    ReassignmentState[] reassignmentStatusList = values();
    for (int i = 0; i < reassignmentStatusList.length; i++) {
      ReassignmentState reassignmentStatus = reassignmentStatusList[i];
      if (reassignmentStatus.status == status) {
        return reassignmentStatus;
      }
    }

    throw new IllegalArgumentException("No matching constant for [" + status + "]");
  }

  public int code() {
    return this.status;
  }
}
