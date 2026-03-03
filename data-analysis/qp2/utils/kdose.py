import math


# --- Helper function for rotisserie factor ---
def _calculate_rotisserie_factor(
        lx: float,
        ly: float,
        lz: float,
        beam_y: float,
        beam_z: float,
        translation_z: float = 0,
) -> tuple[float, bool]:
    """
    Calculates the rotisserie factor, which accounts for the effective
    crystal volume exposed during data collection with crystal rotation.

    Args:
        lx (float): Crystal dimension along X-axis.
        ly (float): Crystal dimension along Y-axis.
        lz (float): Crystal dimension along Z-axis.
        beam_y (float): Beam size along Y-axis.
        beam_z (float): Beam size along Z-axis.
        translation_z (float): Z-translation of the crystal. (Currently not used
                                in the calculation, original code behavior maintained).

    Returns:
        tuple[float, bool]: A tuple containing:
            - rotisserie_factor (float): The calculated rotisserie factor.
            - need_180 (bool): True if a 180-degree rotation is needed to cover
                                the crystal effectively, False otherwise.
    """
    if translation_z >= lz - beam_z:
        translation_z = lz - beam_z

    if translation_z < 0:
        translation_z = 0

    effective_horizontal_coverage = translation_z + beam_z

    effective_vertical_coverage = beam_y  # Initial assumption for vertical coverage

    need_180_rotation = False
    # If beam_y (vertical beam size) is less than crystal_ly or crystal_lx,
    # it implies the beam is smaller than the crystal's cross-section
    # when considering rotation, and thus a 180-degree rotation might be
    # needed to expose the full crystal.
    if effective_vertical_coverage < ly or effective_vertical_coverage < lx:
        need_180_rotation = True
        # The use of sqrt(lx * ly) is an approximation for an "effective"
        # horizontal dimension when rotating. This often implies that the
        # crystal is being rotated to present its largest possible cross-section
        # to the beam.
        effective_vertical_coverage = math.sqrt(lx * ly)

    effective_exposed_area = effective_vertical_coverage * effective_horizontal_coverage
    # The rotisserie factor is the ratio of the effectively exposed crystal area
    # to the actual beam area. This is a common way to account for how much
    # more crystal volume is irradiated than just the instantaneous beam volume.
    rotisserie_factor = effective_exposed_area / (beam_y * beam_z)

    return rotisserie_factor, need_180_rotation


def calculate_crystal_lifetime(
        flux: float = 2e13,  # Photons/second (total flux from source)
        wavelength: float = 1.0,  # Angstroms (X-ray wavelength)
        dose_limit_mgy: float = None,  # MGy (MegaGray) - Maximum tolerable dose
        resolution_angstroms: float = 3.0,  # Angstroms (Target resolution)
        crystal_lx_um: float = 20,  # Micrometers (Crystal dimension along X-axis)
        crystal_ly_um: float = 20,  # Micrometers (Crystal dimension along Y-axis)
        crystal_lz_um: float = 20,  # Micrometers (Crystal dimension along Z-axis)
        beam_size_y_um: float = 20,  # Micrometers (Beam size along Y-axis)
        beam_size_z_um: float = 20,  # Micrometers (Beam size along Z-axis)
        exposure_time_s: float = 0.2,  # Seconds (Exposure time per image)
        # Unitless (Factor for beam attenuation, e.g., due to air/windows)
        attenuation_factor: float = 1.0,
) -> tuple[int, float]:
    """

    Calculates the estimated number of images that can be collected from a crystal
    before it reaches its radiation dose limit, due to James Holton's xtallife
    web server.

    This function is based on principles used in X-ray crystallography to estimate
    crystal lifetime under X-ray exposure.


    directions:
    x --- along x-ray path, thickness of crystal
    y --- vertical, along gravity
    z --- horizontal, along gonio rotation axis

    Args:
        flux (float): Total X-ray flux from the source in photons/second.
        wavelength (float): X-ray wavelength in Angstroms.
        dose_limit_mgy (float, optional): The maximum tolerable radiation dose for
            the crystal in MegaGray (MGy). If not provided, it defaults to
            `resolution_angstroms * 10.0 MGy` based on common empirical rules
            for protein crystallography.
        resolution_angstroms (float): The desired resolution for data collection
            in Angstroms. Used for default dose limit calculation.
        crystal_lx_um (float): Crystal dimension along the X-axis in micrometers.
        crystal_ly_um (float): Crystal dimension along the Y-axis in micrometers.
        crystal_lz_um (float): Crystal dimension along the Z-axis in micrometers.
        beam_size_y_um (float): X-ray beam size along the Y-axis (vertical)
            at the sample position in micrometers.
        beam_size_z_um (float): X-ray beam size along the Z-axis (horizontal)
            at the sample position in micrometers.
        exposure_time_s (float): Exposure time per single image in seconds.
        attenuation_factor (float): A factor representing the attenuation of the
            X-ray beam before it reaches the crystal. A value of 1.0 means no attenuation.

    Returns:
        float: The estimated number of images that can be collected.
    """

    # --- Input Validation and Default Values ---
    if not (isinstance(flux, (int, float)) and flux > 0):
        raise ValueError("Flux must be a positive number.")
    if not (isinstance(wavelength, (int, float)) and wavelength > 0):
        raise ValueError("Wavelength must be a positive number.")
    if not (isinstance(crystal_lx_um, (int, float)) and crystal_lx_um > 0):
        raise ValueError("Crystal X dimension must be a positive number.")
    if not (isinstance(crystal_ly_um, (int, float)) and crystal_ly_um > 0):
        raise ValueError("Crystal Y dimension must be a positive number.")
    if not (isinstance(crystal_lz_um, (int, float)) and crystal_lz_um > 0):
        raise ValueError("Crystal Z dimension must be a positive number.")
    if not (isinstance(beam_size_y_um, (int, float)) and beam_size_y_um > 0):
        raise ValueError("Beam Y size must be a positive number.")
    if not (isinstance(beam_size_z_um, (int, float)) and beam_size_z_um > 0):
        raise ValueError("Beam Z size must be a positive number.")
    if not (isinstance(exposure_time_s, (int, float)) and exposure_time_s > 0):
        raise ValueError("Exposure time must be a positive number.")
    if not (isinstance(attenuation_factor, (int, float)) and attenuation_factor >= 1):
        raise ValueError(
            "Fold of descrease in intensity. Attenuation factor must be >= 1 (inclusive of 1).")
    if not (
            isinstance(resolution_angstroms, (int, float)
                       ) and resolution_angstroms > 0
    ):
        raise ValueError("Resolution must be a positive number.")

    if dose_limit_mgy is None:
        # A common empirical rule for protein crystallography dose limit
        # This relationship (resolution * 10 MGy) is a simplified approximation.
        # More precise calculations (e.g., using RADDOSE) consider material
        # composition and specific damage mechanisms.
        dose_limit_mgy = resolution_angstroms * 10.0
    elif not (isinstance(dose_limit_mgy, (int, float)) and dose_limit_mgy > 0):
        raise ValueError("Dose limit must be a positive number or None.")

    # --- Calculations ---
    # Convert dose limit from MGy to Gy
    dose_limit_gy = dose_limit_mgy * 1e6

    # Calculate flux density (photons/um^2/s)
    # The flux is the total flux, so divide by beam area to get density.
    # Attenuation factor reduces the effective flux.
    flux_density = flux / \
                   (beam_size_y_um * beam_size_z_um) / attenuation_factor

    # kdose (photons/um^2/Gy) is a constant relating fluence to dose.
    # It's empirically derived for protein crystals.
    # K_DOSE_CONST = 2000.0 (photons * Angstrom^2) / (um^2 * Gy)
    # So, kdose = K_DOSE_CONST / (wavelength * wavelength) if wavelength is in Angstroms.
    # If the provided kdose formula `2000.0 / wavelength / wavelength` already accounts
    # for unit conversion (Angstroms to micrometers), then it's fine.
    # Typically, the dose constant for protein crystals is around 2000 photons/um^2/Gy for 1 Angstrom.
    # The formula used here, `2000.0 / wavelength / wavelength`, is a common approximation.
    # It implies that the dose efficiency scales inversely with the square of the wavelength.
    kdose = 2000.0 / (wavelength * wavelength)  # photons / (um^2 * Gy)

    # Dose rate (Gy/s)
    # (Flux density in photons/um^2/s) / (kdose in photons/um^2/Gy) = Gy/s
    dose_rate = flux_density / kdose

    # Calculate rotisserie factor
    # Note: The original code's `translation_z` in `cal_rotisserie_factor` was always 0.
    # If this is intended to model a crystal that is not translated during exposure,
    # the parameter can be removed or explicitly set to 0 in the call.
    # For now, keeping it as 0 to match original behavior.
    rotisserie_factor, _ = _calculate_rotisserie_factor(
        crystal_lx_um,
        crystal_ly_um,
        crystal_lz_um,
        beam_size_y_um,
        beam_size_z_um,
        translation_z=0,
    )

    # Lifetime in seconds (before reaching dose limit, considering rotisserie effect)
    # (Dose limit in Gy) * (Rotisserie factor) / (Dose rate in Gy/s) = seconds
    # The rotisserie factor increases the effective dose tolerance because the dose
    # is spread over a larger effective volume.
    lifetime_s = dose_limit_gy * rotisserie_factor / dose_rate

    # Number of images
    nimages = int(lifetime_s / exposure_time_s)

    return (nimages, lifetime_s)


# --- Example Usage ---
if __name__ == "__main__":
    # Example 1: Default parameters
    print("--- Example 1: Default Parameters ---")
    num_images_default, _ = calculate_crystal_lifetime()
    print(f"Estimated number of images (default): {num_images_default:.2f}\n")

    # Example 2: Larger crystal, higher flux
    print("--- Example 2: Larger Crystal, Higher Flux ---")
    num_images_large_crystal, _ = calculate_crystal_lifetime(
        flux=5e14,
        crystal_lx_um=50,
        crystal_ly_um=50,
        crystal_lz_um=50,
        beam_size_y_um=30,
        beam_size_z_um=30,
        exposure_time_s=0.1,
        resolution_angstroms=2.5,
    )
    print(
        f"Estimated number of images (large crystal, high flux): {num_images_large_crystal:.2f}\n"
    )

    # Example 3: Specific dose limit provided
    print("--- Example 3: Specific Dose Limit ---")
    num_images_specific_dose, _ = calculate_crystal_lifetime(
        flux=1e13,
        wavelength=1.54,  # Cu K-alpha
        dose_limit_mgy=20.0,  # Directly setting dose limit to 20 MGy
        crystal_lx_um=10,
        crystal_ly_um=10,
        crystal_lz_um=10,
        beam_size_y_um=10,
        beam_size_z_um=10,
        exposure_time_s=0.5,
    )
    print(
        f"Estimated number of images (specific dose limit): {num_images_specific_dose:.2f}\n"
    )

    # Example 4: Testing attenuation
    print("--- Example 4: With Attenuation ---")
    num_images_attenuated, _ = calculate_crystal_lifetime(
        flux=2e13,
        attenuation_factor=100,  # 20% attenuation
    )
    print(
        f"Estimated number of images (with 20% attenuation): {num_images_attenuated:.2f}\n"
    )

    # Example 5: Smaller crystal, larger beam (less efficient use of photons)
    print("--- Example 5: Smaller Crystal, Larger Beam ---")
    num_images_small_crystal_large_beam, _ = calculate_crystal_lifetime(
        crystal_lx_um=10,
        crystal_ly_um=10,
        crystal_lz_um=10,
        beam_size_y_um=50,
        beam_size_z_um=50,
        flux=1e13,
    )
    print(
        f"Estimated number of images (small crystal, large beam): {num_images_small_crystal_large_beam:.2f}\n"
    )

    # Example with a potential ValueError
    # try:
    #     calculate_crystal_lifetime(flux=-1e13)
    # except ValueError as e:
    #     print(f"Error: {e}")
