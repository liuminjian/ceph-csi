package rbdrestore

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

	rbdv1 "github.com/ceph/ceph-csi/api/rbd/v1"
	ctrl "github.com/ceph/ceph-csi/internal/controller"
	"github.com/ceph/ceph-csi/internal/controller/utils"
	"github.com/ceph/ceph-csi/internal/util"
)

type ReconcileRBDRestore struct {
	client client.Client
	config ctrl.Config
	Locks  *util.VolumeLocks
}

// Init will add the ReconcileRBDRestore to the list.
func Init() {
	// add ReconcileRBDRestore to the list
	ctrl.ControllerList = append(ctrl.ControllerList, &ReconcileRBDRestore{})
}

func (r *ReconcileRBDRestore) Add(mgr manager.Manager, config ctrl.Config) error {
	return add(mgr, newRBDRestoreReconciler(mgr, config))
}

func newRBDRestoreReconciler(mgr manager.Manager, config ctrl.Config) reconcile.Reconciler {
	r := &ReconcileRBDRestore{
		mgr.GetClient(),
		config,
		util.NewVolumeLocks(),
	}

	return r
}

func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New(
		"rbdrestore-controller",
		mgr,
		controller.Options{MaxConcurrentReconciles: 1, Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to RBDRestore
	err = c.Watch(&source.Kind{Type: &rbdv1.RBDRestore{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return fmt.Errorf("failed to watch the changes: %w", err)
	}

	return nil
}

func (r *ReconcileRBDRestore) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	bk := &rbdv1.RBDRestore{}
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

	err = r.reconcileRestore(ctx, bk)
	if err != nil {
		return reconcile.Result{}, err
	}

	return reconcile.Result{}, nil
}

func (r *ReconcileRBDRestore) reconcileRestore(ctx context.Context, restore *rbdv1.RBDRestore) (err error) {
	if restore.Status.Phase != rbdv1.RSTRBDStatusInit {
		return
	}

	err = r.CreateRestore(ctx, restore)
	if err == nil {
		klog.Infof("restore %s done %s", restore.Name, restore.Spec.BackupName)
		err = r.UpdateRspInfo(restore, rbdv1.RSTRBDStatusDone)
	} else {
		klog.Errorf("restore %s failed %s err %v", restore.Name, restore.Spec.BackupName, err)
		err = r.UpdateRspInfo(restore, rbdv1.RSTRBDStatusFailed)
	}

	return
}

func (r *ReconcileRBDRestore) CreateRestore(ctx context.Context, restore *rbdv1.RBDRestore) (err error) {
	// TODO 快照不存在则创建

	bk := &rbdv1.RBDBackup{}
	err = r.client.Get(ctx, types.NamespacedName{Name: restore.Spec.BackupName, Namespace: restore.Namespace}, bk)
	if err != nil {
		errStr := fmt.Sprintf("find backup failed: %s", err.Error())
		util.ErrorLogMsg(errStr)
		return
	}

	if bk.Status.Phase != rbdv1.BKPRBDStatusDone {
		err = fmt.Errorf("backup is not done")
		util.ErrorLogMsg(err.Error())
		return
	}

	poolName := bk.Status.Pool
	imageName := bk.Status.ImageName
	secretName := bk.Status.SecretName
	secretNamespace := bk.Status.SecretNamespace
	monitors := bk.Status.Monitors

	cr, err := utils.GetCredentials(ctx, r.client, secretName, secretNamespace)
	if err != nil {
		util.ErrorLogMsg("failed to get credentials from secret %s", err)
		return
	}
	defer cr.DeleteCredentials()

	args, err := r.buildVolumeRestoreArgs(restore.Spec.RestoreSrc, poolName, imageName, monitors, cr)
	if err != nil {
		return err
	}
	cmd := exec.Command("bash", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		err = fmt.Errorf("rbd: could not restore the volume %v cmd %v output: %s, err: %s",
			restore.Name, args, string(out), err.Error())
		util.ErrorLogMsg(err.Error())
	}

	return
}

func (r *ReconcileRBDRestore) buildVolumeRestoreArgs(restoreSrc string, pool string, image string,
	monitor string, cr *util.Credentials) ([]string, error) {
	var RBDVolArg []string
	rstrAddr := strings.Split(restoreSrc, ":")
	if len(rstrAddr) != 2 {
		return RBDVolArg, fmt.Errorf("rbd: invalid restore server address %s", restoreSrc)
	}

	restoreSource := "nc -w 3 " + rstrAddr[0] + " " + rstrAddr[1] + " | "

	cmd := fmt.Sprintf("%s %s %s -m %s --id %s -K %s - %s/%s",
		restoreSource, utils.RBDVolCmd, utils.RBDImportArg, monitor, cr.ID, cr.KeyFile, pool, image)

	RBDVolArg = append(RBDVolArg, "-c", cmd)

	return RBDVolArg, nil
}

func (r *ReconcileRBDRestore) UpdateRspInfo(restore *rbdv1.RBDRestore, phase rbdv1.RBDRestoreStatusPhase) (err error) {
	restoreCopy := restore.DeepCopy()
	// controllerutil.AddFinalizer(restoreCopy, utils.RBDFinalizer)
	restoreCopy.Status.Phase = phase
	if !reflect.DeepEqual(restoreCopy, restore) {
		return r.client.Patch(context.TODO(), restore, client.MergeFrom(restoreCopy))
	}
	return
}
