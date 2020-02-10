package pxf_test

import (
	"bytes"
	"errors"
	"os"
	"pxf-cli/pxf"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("CommandFunc", func() {
	Context("when PXF_CONF and GPHOME are set", func() {
		BeforeEach(func() {
			_ = os.Setenv("GPHOME", "/test/gphome")
			_ = os.Setenv("PXF_CONF", "/test/gphome/pxf_conf")
		})

		It("successfully generates init command", func() {
			commandFunc, err := pxf.InitCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			expected := "PXF_CONF=/test/gphome/pxf_conf /test/gphome/pxf/bin/pxf init"
			Expect(commandFunc("foo")).To(Equal(expected))
		})
	})

	Context("when only GPHOME is set", func() {
		BeforeEach(func() {
			_ = os.Setenv("GPHOME", "/test/gphome")
			_ = os.Unsetenv("PXF_CONF")
		})

		It("successfully generates start, stop, status, restart, and reset commands", func() {
			commandFunc, err := pxf.StartCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect(commandFunc("foo")).To(Equal("/test/gphome/pxf/bin/pxf start"))

			commandFunc, err = pxf.StopCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect(commandFunc("foo")).To(Equal("/test/gphome/pxf/bin/pxf stop"))

			commandFunc, err = pxf.StatusCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect(commandFunc("foo")).To(Equal("/test/gphome/pxf/bin/pxf status"))

			commandFunc, err = pxf.RestartCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect(commandFunc("foo")).To(Equal("/test/gphome/pxf/bin/pxf restart"))

			commandFunc, err = pxf.ResetCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect(commandFunc("foo")).To(Equal("/test/gphome/pxf/bin/pxf reset --force"))
		})
		It("fails to init or sync", func() {
			commandFunc, err := pxf.InitCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_CONF must be set")))

			commandFunc, err = pxf.SyncCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_CONF must be set")))
		})
	})
	Context("when only PXF_CONF is set", func() {
		BeforeEach(func() {
			_ = os.Setenv("PXF_CONF", "/test/gphome/pxf_conf")
			_ = os.Unsetenv("GPHOME")
		})

		It("sets up rsync commands of $PXF_CONF/{conf,lib,servers} dirs", func() {
			commandFunc, err := pxf.SyncCommand.GetFunctionToExecute()
			Expect(err).To(BeNil())
			Expect(commandFunc("sdw1")).To(Equal(
				"rsync -az -e 'ssh -o StrictHostKeyChecking=no' " +
					"'/test/gphome/pxf_conf/conf' " +
					"'/test/gphome/pxf_conf/lib' " +
					"'/test/gphome/pxf_conf/servers' " +
					"'sdw1:/test/gphome/pxf_conf'",
			))
			Expect(commandFunc("sdw2")).To(Equal(
				"rsync -az -e 'ssh -o StrictHostKeyChecking=no' " +
					"'/test/gphome/pxf_conf/conf' " +
					"'/test/gphome/pxf_conf/lib' " +
					"'/test/gphome/pxf_conf/servers' " +
					"'sdw2:/test/gphome/pxf_conf'",
			))
		})

		It("fails to init, start, stop, restart, or tell status", func() {
			commandFunc, err := pxf.InitCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("GPHOME must be set")))
			commandFunc, err = pxf.StartCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("GPHOME must be set")))
			commandFunc, err = pxf.StopCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("GPHOME must be set")))
			commandFunc, err = pxf.RestartCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("GPHOME must be set")))
			commandFunc, err = pxf.StatusCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("GPHOME must be set")))
		})
	})

	Context("when PXF_CONF is set to empty string", func() {
		BeforeEach(func() {
			_ = os.Setenv("PXF_CONF", "")
			_ = os.Setenv("GPHOME", "/test/gphome")
		})
		It("fails to init or sync", func() {
			_ = os.Setenv("PXF_CONF", "")
			commandFunc, err := pxf.InitCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_CONF cannot be blank")))
			commandFunc, err = pxf.SyncCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("PXF_CONF cannot be blank")))
		})
	})

	Context("when GPHOME is set to empty string", func() {
		BeforeEach(func() {
			_ = os.Setenv("GPHOME", "")
			_ = os.Unsetenv("PXF_CONF")
		})
		It("fails to init, start, stop, restart, or status", func() {
			_ = os.Setenv("GPHOME", "")
			commandFunc, err := pxf.InitCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("GPHOME cannot be blank")))
			commandFunc, err = pxf.StartCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("GPHOME cannot be blank")))
			commandFunc, err = pxf.StopCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("GPHOME cannot be blank")))
			commandFunc, err = pxf.RestartCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("GPHOME cannot be blank")))
			commandFunc, err = pxf.StatusCommand.GetFunctionToExecute()
			Expect(commandFunc).To(BeNil())
			Expect(err).To(Equal(errors.New("GPHOME cannot be blank")))
		})
	})

	Context("When the user tries to run a warn command and doesn't answer y", func() {
		It("Returns an error", func() {
			var input bytes.Buffer
			input.Write([]byte(""))
			err := pxf.ResetCommand.Warn(&input)
			Expect(err).To(Equal(errors.New("pxf reset cancelled")))
		})
	})

	Context("When the user tries to run a warn command and they answer y", func() {
		It("Returns an error", func() {
			var input bytes.Buffer
			input.Write([]byte("Y"))
			err := pxf.ResetCommand.Warn(&input)
			Expect(err).To(BeNil())
		})
	})

	Context("When the user tries to run a non-warn command", func() {
		It("Returns an error", func() {
			var input bytes.Buffer
			input.Write([]byte("this input shouldn't matter!"))
			err := pxf.StatusCommand.Warn(&input)
			Expect(err).To(BeNil())
		})
	})

})
