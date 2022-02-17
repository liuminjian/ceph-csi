package rbdrestore

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"

	rbdv1 "github.com/ceph/ceph-csi/api/rbd/v1"
	"github.com/ceph/ceph-csi/internal/controller"
	"github.com/ceph/ceph-csi/internal/controller/utils"
	"github.com/ceph/ceph-csi/internal/util"
)

type RestoreTask struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	restore    *rbdv1.RBDRestore
	locks      *util.VolumeLocks
	cr         *util.Credentials
	monitor    string
	clusterId  string
	cmd        *exec.Cmd
	buf        bytes.Buffer
	isRunning  bool
}

func NewRestoreTask(ctx context.Context, restore *rbdv1.RBDRestore, locks *util.VolumeLocks,
	cr *util.Credentials, monitor string, clusterId string) controller.TaskJob {
	cancelCtx, cancelFunc := context.WithCancel(ctx)
	return &RestoreTask{ctx: cancelCtx, cancelFunc: cancelFunc, restore: restore, locks: locks, cr: cr,
		monitor: monitor, clusterId: clusterId}
}

func (r *RestoreTask) Running() bool {
	return r.isRunning
}

func (r *RestoreTask) Success() bool {
	return !r.Running() && r.cmd.ProcessState.Success() && !strings.Contains(r.buf.String(), "error")
}

func (r *RestoreTask) Start() error {
	ctx := context.TODO()
	src := r.restore.Spec.RestoreSrc
	pool := r.restore.Spec.Pool
	imageName := r.restore.Spec.ImageName

	purgeArgs := r.buildVolumePurgeArgs(pool, imageName, r.monitor, r.cr)
	cmd := exec.Command("bash", purgeArgs...)
	util.UsefulLog(r.ctx, "purge: %v", purgeArgs)
	out, err := cmd.CombinedOutput()
	if err != nil {
		err = fmt.Errorf("rbd: could not purge the volume %v cmd %v output: %s, err: %s, exit code: %d",
			r.restore.Name, purgeArgs, string(out), err.Error(), cmd.ProcessState.ExitCode())
		util.ErrorLogMsg(err.Error())
		if cmd.ProcessState.ExitCode() == 16 {
			return controller.InUseError{Err: "image already in use"}
		} else {
			return err
		}
	}

	removeArgs := r.buildVolumeRemoveArgs(pool, imageName, r.monitor, r.cr)
	cmd = exec.Command("bash", removeArgs...)
	util.UsefulLog(r.ctx, "restore rm: %v", removeArgs)
	out, err = cmd.CombinedOutput()
	if err != nil {
		err = fmt.Errorf("rbd: could not restore the volume %v cmd %v output: %s, err: %s, exit code: %d",
			r.restore.Name, removeArgs, string(out), err.Error(), cmd.ProcessState.ExitCode())
		util.ErrorLogMsg(err.Error())
		if cmd.ProcessState.ExitCode() == 16 {
			return controller.InUseError{Err: "image already in use"}
		}
	}

	args, err := r.buildVolumeRestoreArgs(src, pool, imageName, r.monitor, r.cr)
	if err != nil {
		return err
	}
	cmd = exec.CommandContext(r.ctx, "bash", args...)
	util.UsefulLog(r.ctx, "restore command: %v", args)
	cmd.Stdout = &r.buf
	cmd.Stderr = &r.buf
	err = cmd.Start()
	if err != nil {
		err = fmt.Errorf("rbd: could not restore the volume %v cmd %v output: %s, err: %s",
			r.restore.Name, args, string(out), err.Error())
		util.ErrorLogMsg(err.Error())
	}
	r.isRunning = true
	go func() {
		cmd.Wait()
		r.isRunning = false
		util.UsefulLog(ctx, fmt.Sprintf("%s %s", imageName, r.buf.String()))
	}()
	r.cmd = cmd
	return err
}

func (r *RestoreTask) Stop() {
	r.cr.DeleteCredentials()
	r.cancelFunc()
}

func (r *RestoreTask) Error() error {
	return fmt.Errorf(r.buf.String())
}

func (r *RestoreTask) buildVolumeRestoreArgs(restoreSrc string, pool string, image string,
	monitor string, cr *util.Credentials) ([]string, error) {
	var RBDVolArg []string
	rstrAddr := strings.Split(restoreSrc, ":")
	if len(rstrAddr) != 2 {
		return RBDVolArg, fmt.Errorf("rbd: invalid restore server address %s", restoreSrc)
	}

	restoreSource := "nc -w 30 -v " + rstrAddr[0] + " " + rstrAddr[1] + " | gzip -d | "

	cmd := fmt.Sprintf("%s %s %s --id %s --keyfile=%s -m %s - %s/%s",
		restoreSource, utils.RBDVolCmd, utils.RBDImportArg, cr.ID, cr.KeyFile, monitor, pool, image)

	RBDVolArg = append(RBDVolArg, "-c", cmd)

	return RBDVolArg, nil
}

func (r *RestoreTask) buildVolumeRemoveArgs(pool string, image string, monitor string,
	cr *util.Credentials) []string {
	cmd := fmt.Sprintf("%s %s --id %s --keyfile=%s -m %s %s/%s", utils.RBDVolCmd, utils.RBDTrashMoveArg,
		cr.ID, cr.KeyFile, monitor, pool, image)
	var RBDVolArg []string
	RBDVolArg = append(RBDVolArg, "-c", cmd)
	return RBDVolArg
}

func (r *RestoreTask) buildVolumePurgeArgs(pool string, image string, monitor string,
	cr *util.Credentials) []string {
	cmd := fmt.Sprintf("%s %s --id %s --keyfile=%s -m %s %s/%s", utils.RBDVolCmd, utils.RBDPurgeArg,
		cr.ID, cr.KeyFile, monitor, pool, image)
	var RBDVolArg []string
	RBDVolArg = append(RBDVolArg, "-c", cmd)
	return RBDVolArg
}
