package rbdbackup

import (
	"context"
	"fmt"
	"os/exec"
	"reflect"
	"strings"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	corev1 "k8s.io/api/core/v1"

	rbdv1 "github.com/ceph/ceph-csi/api/rbd/v1"
	ctrl "github.com/ceph/ceph-csi/internal/controller"
	"github.com/ceph/ceph-csi/internal/controller/utils"
	"github.com/ceph/ceph-csi/internal/util"
)

type ReconcileRBDBackup struct {
	client client.Client
	config ctrl.Config
	Locks  *util.VolumeLocks
}

// Init will add the ReconcileRBDBackup to the list.
func Init() {
	// add ReconcileRBDBackup to the list
	ctrl.ControllerList = append(ctrl.ControllerList, &ReconcileRBDBackup{})
}

func (r *ReconcileRBDBackup) Add(mgr manager.Manager, config ctrl.Config) error {
	return add(mgr, newRBDBackupReconciler(mgr, config))
}

func newRBDBackupReconciler(mgr manager.Manager, config ctrl.Config) reconcile.Reconciler {
	r := &ReconcileRBDBackup{
		mgr.GetClient(),
		config,
		util.NewVolumeLocks(),
	}

	return r
}

func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New(
		"rbdbackup-controller",
		mgr,
		controller.Options{MaxConcurrentReconciles: 1, Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to RBDBackup
	err = c.Watch(&source.Kind{Type: &rbdv1.RBDBackup{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return fmt.Errorf("failed to watch the changes: %w", err)
	}

	return nil
}

func (r *ReconcileRBDBackup) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	bk := &rbdv1.RBDBackup{}
	err := r.client.Get(ctx, request.NamespacedName, bk)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}

		return reconcile.Result{}, err
	}
	// Check if the object is under deletion
	if !bk.GetDeletionTimestamp().IsZero() {
		return reconcile.Result{}, nil
	}

	err = r.reconcileBackup(ctx, bk)
	if err != nil {
		return reconcile.Result{}, err
	}

	return reconcile.Result{}, nil
}

func (r *ReconcileRBDBackup) reconcileBackup(ctx context.Context, backup *rbdv1.RBDBackup) (err error) {
	if backup.Status.Phase != rbdv1.BKPRBDStatusInit {
		return
	}

	err = r.CreateBackup(ctx, backup)
	if err == nil {
		klog.Infof("backup %s done %s@%s", backup.Name, backup.Spec.VolumeName, backup.Spec.SnapshotName)
		err = r.UpdateBkpInfo(backup, rbdv1.BKPRBDStatusDone)
	} else {
		klog.Errorf("backup %s failed %s@%s err %v", backup.Name, backup.Spec.VolumeName,
			backup.Spec.SnapshotName, err)
		err = r.UpdateBkpInfo(backup, rbdv1.BKPRBDStatusFailed)
	}

	return
}

func (r *ReconcileRBDBackup) CreateBackup(ctx context.Context, backup *rbdv1.RBDBackup) (err error) {
	// TODO 快照不存在则创建

	pv := &corev1.PersistentVolume{}
	err = r.client.Get(ctx, types.NamespacedName{Name: backup.Spec.VolumeName}, pv)
	if err != nil {
		errStr := fmt.Sprintf("find pv failed: %s", err.Error())
		util.ErrorLogMsg(errStr)
		return
	}

	if pv.Spec.CSI == nil || len(pv.Spec.CSI.VolumeAttributes["pool"]) == 0 {
		err = fmt.Errorf("pv volumeAttributes missing pool or imageName or secret")
		util.ErrorLogMsg(err.Error())
		return
	}

	poolName := pv.Spec.CSI.VolumeAttributes["pool"]
	snapshotName := backup.Spec.SnapshotName
	// Take lock to process only one snapshotHandle at a time.
	if ok := r.Locks.TryAcquire(snapshotName); !ok {
		return fmt.Errorf(util.VolumeOperationAlreadyExistsFmt, snapshotName)
	}
	defer r.Locks.Release(snapshotName)

	cr, err := utils.GetCredentials(ctx, r.client, r.config.SecretName, r.config.SecretNamespace)
	if err != nil {
		util.ErrorLogMsg("failed to get credentials from secret %s", err)
		return
	}
	defer cr.DeleteCredentials()

	monitors, _, err := util.FetchMappedClusterIDAndMons(ctx, r.config.ClusterId)
	if err != nil {
		util.ErrorLogMsg(err.Error())
		return
	}
	args, err := r.buildVolumeBackupArgs(backup.Spec.BackupDest, poolName, snapshotName, monitors, cr)
	if err != nil {
		return err
	}
	cmd := exec.Command("bash", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		err = fmt.Errorf("rbd: could not backup the volume %v cmd %v output: %s, err: %s",
			pv.Name, args, string(out), err.Error())
		util.ErrorLogMsg(err.Error())
	}

	backup.Status.Pool = poolName
	backup.Status.ImageName = pv.Spec.CSI.VolumeAttributes["imageName"]
	return
}

func (r *ReconcileRBDBackup) buildVolumeBackupArgs(backupDest string, pool string, image string,
	monitor string, cr *util.Credentials) ([]string, error) {
	var RBDVolArg []string
	bkpAddr := strings.Split(backupDest, ":")
	if len(bkpAddr) != 2 {
		return RBDVolArg, fmt.Errorf("rbd: invalid backup server address %s", backupDest)
	}

	remote := " | gzip | nc -w 3 " + bkpAddr[0] + " " + bkpAddr[1]

	cmd := fmt.Sprintf("%s %s %s/%s -m %s --id %s -K %s - %s", utils.RBDVolCmd, utils.RBDExportArg,
		pool, image, monitor, cr.ID, cr.KeyFile, remote)

	RBDVolArg = append(RBDVolArg, "-c", cmd)

	return RBDVolArg, nil
}

func (r *ReconcileRBDBackup) UpdateBkpInfo(backup *rbdv1.RBDBackup, phase rbdv1.RBDBackupStatusPhase) (err error) {
	backupCopy := backup.DeepCopy()
	// controllerutil.AddFinalizer(backupCopy, utils.RBDFinalizer)
	backupCopy.Status.Phase = phase
	if !reflect.DeepEqual(backupCopy, backup) {
		return r.client.Patch(context.TODO(), backup, client.MergeFrom(backupCopy))
	}
	return
}
