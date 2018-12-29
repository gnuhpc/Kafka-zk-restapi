package org.gnuhpc.bigdata.constant;

public enum ReassignmentStatus {
  ReassignmentFailed(-1, "Reassignment Failed"),
  ReassignmentInProgress(0, "Reassignment In Progress"),
  ReassignmentCompleted(1, "Reassignment Completed");

  private int status;
  private String msg;

  private ReassignmentStatus(int status, String msg) {
    this.status = status;
    this.msg = msg;
  }

  public static ReassignmentStatus valueOf(int status) {
    ReassignmentStatus[] reassignmentStatusList = values();
    for (int i = 0; i < reassignmentStatusList.length; i++) {
      ReassignmentStatus reassignmentStatus = reassignmentStatusList[i];
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
