from sqlalchemy import and_, delete
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from mapadroid.db.model import MadminInstance, MadApk, FilestoreMeta
from mapadroid.mad_apk import APKArch, APKType, MADPackages, MADPackage


class MadApkHelper:
    @staticmethod
    async def get_current_version(session: AsyncSession, package: APKType, architecture: APKArch) -> Optional[str]:
        """
        Get the currently available version of the package / architecture
        Args:
            session:
            package:
            architecture:

        Returns:

        """
        stmt = select(MadApk).where(and_(MadApk.usage == package.value,
                                         MadApk.arch == architecture.value))
        result = await session.execute(stmt)
        apk: Optional[MadApk] = result.scalars().first()
        return apk.version if apk is not None else None

    @staticmethod
    async def get_current_package_info(session: AsyncSession, package: APKType) -> Optional[MADPackages]:
        data = MADPackages()
        stmt = select(MadApk.version, MadApk.arch, FilestoreMeta.filename, FilestoreMeta.size, FilestoreMeta.mimetype)\
            .select_from(MadApk)\
            .join(FilestoreMeta, MadApk.filestore_id == FilestoreMeta.filestore_id)\
            .where(MadApk.usage == package.value)
        result = await session.execute(stmt)
        for row in result:
            arch = row['arch']
            row['arch_disp'] = APKArch(arch).name
            row['usage_disp'] = APKType(package).name
            data[APKArch(arch)] = MADPackage(APKType(package), APKArch(arch), **row)
        return data if data else None

    @staticmethod
    async def delete_file(session: AsyncSession, package: APKType, architecture: APKArch) -> bool:
        """
        Remove the package/filestore entry and update the configuration
        Args:
            session:
            package:
            architecture:

        Returns:
        """
        stmt = select(MadApk).where(and_(MadApk.usage == package.value,
                                         MadApk.arch == architecture.value))
        result = await session.execute(stmt)
        apk: Optional[MadApk] = result.scalars().first()
        if not apk or not apk.filestore_id:
            return False
        del_stmt = delete(FilestoreMeta).where(FilestoreMeta.filestore_id == apk.filestore_id)
        await session.execute(del_stmt)