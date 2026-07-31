import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class ProcessorDeploymentContractTest(unittest.TestCase):
    def test_direct_deployment_uses_the_shared_versioned_repository_image(self):
        source = (ROOT / "deploy" / "deploy_processor.sh").read_text()
        self.assertIn("OCI_REGISTRY_REPOSITORY_ID", source)
        self.assertIn("artifacts container repository get --repository-id", source)
        self.assertIn('PROCESSOR_REGISTRY_IMAGE_TAG="processor-$PROCESSOR_IMAGE_TAG"', source)
        self.assertIn(
            'IMAGE="$REGION_KEY.ocir.io/$NAMESPACE/$OCI_REGISTRY_REPOSITORY:$PROCESSOR_REGISTRY_IMAGE_TAG"',
            source,
        )
        self.assertIn("Processor image is not published:", source)
        self.assertNotIn(
            '${REPOSITORY_PREFIX,,}/$PROCESSOR_IMAGE_NAME:$PROCESSOR_IMAGE_TAG',
            source,
        )

    def test_deployment_history_uses_the_deployed_repository_and_tag(self):
        source = (ROOT / "deploy" / "deploy_processor.sh").read_text()
        self.assertIn('--image-name "$OCI_REGISTRY_REPOSITORY"', source)
        self.assertIn('--image-tag "$PROCESSOR_REGISTRY_IMAGE_TAG"', source)

    def test_flow_verifier_uses_the_same_shared_repository_contract(self):
        source = (ROOT / "tests" / "integration" / "verify_flow.py").read_text()
        self.assertIn('required("OCI_REGISTRY_REPOSITORY")', source)
        self.assertIn('image_tag = f"processor-{image_tag}"', source)
        self.assertIn('{repository}:{image_tag}', source)
        self.assertNotIn('required("REPOSITORY_PREFIX")', source)
        self.assertNotIn('required("PROCESSOR_IMAGE_NAME")', source)


if __name__ == "__main__":
    unittest.main()
